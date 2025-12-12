using AppViewLite.Models;
using AppViewLite.PluggableProtocols;
using AppViewLite.Web.Components;
using FishyFlip;
using FishyFlip.Lexicon;
using FishyFlip.Models;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.Http.Extensions;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.HttpOverrides;
using Microsoft.AspNetCore.HttpsPolicy;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.WebUtilities;
using AppViewLite.Storage;
using System.Diagnostics;
using System.IdentityModel.Tokens.Jwt;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json.Serialization;

namespace AppViewLite.Web
{
    public static class Program
    {

        public static async Task Main(string[] args)
        {
            AppContext.SetSwitch("Microsoft.AspNetCore.Components.Routing.NavLink.EnableMatchAllForQueryStringAndFragment", true);
            var apis = AppViewLiteInit.Init(args);
            var relationships = apis.DangerousUnlockedRelationships;
            var listenToFirehose = AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_LISTEN_TO_FIREHOSE) ?? true;
            var builder = WebApplication.CreateBuilder(args);
            var bindUrls = AppViewLiteConfiguration.GetStringList(AppViewLiteParameter.APPVIEWLITE_BIND_URLS) ?? ["https://localhost:61749", "http://localhost:61750"];
            builder.WebHost.UseUrls(bindUrls);
            // Add services to the container.
            builder.Logging.ClearProviders();
            builder.Logging.AddProvider(new LogWrapper.Provider());
            builder.Services.AddRazorComponents();
            builder.Services.AddRazorPages();
            builder.Services.Configure<ForwardedHeadersOptions>(options =>
            {
                options.ForwardedHeaders =
                    ForwardedHeaders.XForwardedFor | ForwardedHeaders.XForwardedProto;
            });
            builder.Services.AddSingleton<BlueskyEnrichedApis>(_ => apis);
            builder.Services.ConfigureHttpJsonOptions(options =>
            {
                options.SerializerOptions.DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull;
            });

            builder.Services.AddCors(options =>
            {
                options.AddPolicy("BskyClient", b => b.WithOrigins("http://localhost:19006").AllowAnyHeader().AllowAnyMethod());
            });

            builder.Services.AddHttpContextAccessor();

            builder.Services.AddScoped(provider =>
            {
                var httpContext = provider.GetRequiredService<IHttpContextAccessor>().HttpContext;
                if (httpContext?.Request?.Path.StartsWithSegments("/ErrorHttpStatus") == true) return AppViewLiteSession.CreateAnonymous();
                var session = TryGetSession(httpContext) ?? AppViewLiteSession.CreateAnonymous();
                return session;
            });
            builder.Services.AddScoped(provider =>
            {
                var session = provider.GetRequiredService<AppViewLiteSession>();
                var httpContext = provider.GetRequiredService<IHttpContextAccessor>().HttpContext;
                var request = httpContext?.Request;
                if (request?.Path.StartsWithSegments("/ErrorHttpStatus") == true) { return RequestContext.CreateForRequest("ErrorHttpStatus"); }
                var signalrConnectionId = request?.Headers["X-AppViewLiteSignalrId"].FirstOrDefault();
                var urgent = request?.Method == "CONNECT" ? false : (request?.Headers["X-AppViewLiteUrgent"].FirstOrDefault() != "0");

                var ctx = RequestContext.CreateForRequest("ServedRequest", session, string.IsNullOrEmpty(signalrConnectionId) ? null : signalrConnectionId, urgent: urgent, requestUrl: httpContext?.Request.GetEncodedPathAndQuery());
                return ctx;
            });
            builder.Services.AddSignalR();

            var bindHttps = bindUrls.FirstOrDefault(x => x.StartsWith("https://", StringComparison.Ordinal));
            if (bindHttps != null)
            {
                var port = bindHttps.Split(':').ElementAtOrDefault(2)?.Replace("/", null);
                if (port != null)
                {
                    builder.Services.Configure<HttpsRedirectionOptions>(options =>
                    {
                        options.HttpsPort = int.Parse(port);
                    });
                }
            }

            var app = builder.Build();

            StaticServiceProvider = app.Services;

#if DEBUG
            apis.BeforeLockEnter += (ctx) =>
            {
                if (ctx != null) return;
                var httpContext = StaticServiceProvider.GetRequiredService<IHttpContextAccessor>()?.HttpContext;
                if (httpContext != null)
                {
                    LoggableBase.Log("Ctx was not passed for " + httpContext.Request.GetEncodedPathAndQuery());
                    // we might have forgotten to pass a ctx
                }
            };
#endif
            app.Lifetime.ApplicationStopping.Register(apis.NotifyShutdownRequested);



            // Configure the HTTP request pipeline.
            if (app.Environment.IsDevelopment())
            {
                //app.UseWebAssemblyDebugging();
            }
            else
            {
                app.UseExceptionHandler("/Error");
                // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
                app.UseHsts();
            }
            app.UseForwardedHeaders();

            app.Use(async (context, next) =>
            {
                if (!(context.Request.Method is "GET" or "HEAD" or "OPTIONS") &&
                    context.Request.Headers.TryGetValue("Sec-Fetch-Site", out var fetchSite) &&
                    fetchSite == "cross-site")
                {
                    var origin = context.Request.Headers.Origin.FirstOrDefault();
                    var requestHost = context.Request.Host.Host;
                    if (requestHost != null && origin != null && new Uri(origin).Host == requestHost)
                    {
                        // OK, this is social-app POSTing on AppViewLite (same server but different port)
                    }
                    else
                    {
                        context.Response.StatusCode = 403;
                        await context.Response.WriteAsync("Cross-site POST requests are not allowed.");
                        return;
                    }
                }

                await next();
            });



            if (bindHttps != null)
                app.UseHttpsRedirection();


            app.UseStatusCodePagesWithReExecute("/ErrorHttpStatus", "?code={0}");
            app.UseAntiforgery();

            app.Use(async (ctx, req) =>
            {
                await req();
                var requestContext = ctx.RequestServices.GetService<RequestContext>();
                if (requestContext != null)
                {
                    requestContext.CompletionTimeStopwatchTicks = Stopwatch.GetTimestamp();
                }
            });

            app.MapStaticAssets();
            app.Use(async (ctx, req) =>
            {
                if (!IsKnownCacheablePath(ctx.Request.Path))
                {
                    ctx.Response.Headers.CacheControl = "no-store, no-cache, must-revalidate";
                    ctx.Response.Headers.Pragma = "no-cache";
                    ctx.Response.Headers.Expires = "0";
                }

                var reqCtx = ctx.RequestServices.GetRequiredService<RequestContext>();
                if (reqCtx.IsLoggedIn)
                {
                    var userCtx = reqCtx.UserContext;
                    var refreshTokenExpire = userCtx.RefreshTokenExpireDate;
                    if (refreshTokenExpire == null)
                    {
                        userCtx.UpdateRefreshTokenExpireDate();
                        refreshTokenExpire = userCtx.RefreshTokenExpireDate;
                    }

                    if ((refreshTokenExpire!.Value - DateTime.UtcNow).TotalDays < 1)
                    {
                        if (ctx.Request.Method == "GET" && ctx.Request.Headers["sec-fetch-dest"].FirstOrDefault() != "empty")
                        {
                            // the refresh token will expire soon. require a new login to avoid broken POST/like later.
                            apis.LogOut(new(reqCtx.Session.Did!, reqCtx.Session.SessionToken!), reqCtx);
                            ctx.Response.Redirect("/login?return=" + Uri.EscapeDataString(ctx.Request.GetEncodedPathAndQuery()));
                            return;
                        }
                    }


                }

                var path = ctx.Request.Path.Value?.ToString();
                if (path != null)
                {
                    var s = path.AsSpan(1);
                    if (s.EndsWith('/') && !s.StartsWith("https://") && !s.StartsWith("http://"))
                    {
                        ctx.Response.Redirect(string.Concat(path.AsSpan(0, path.Length - 1), ctx.Request.QueryString.ToString()));
                        return;
                    }
                    var slash = s.IndexOf('/');
                    var firstSegment = slash != -1 ? s.Slice(0, slash) : s;

                    if (!firstSegment.StartsWith('@'))
                    {
                        if (firstSegment.StartsWith("did:"))
                        {
                            ctx.Response.Redirect("/@" + firstSegment.ToString() + ctx.Request.QueryString.Value);
                            return;
                        }
                        if (
                            !StringUtils.IsKnownFileTypeAsOpposedToTld(firstSegment.Slice(firstSegment.LastIndexOf('.') + 1)) &&
                            BlueskyEnrichedApis.IsValidDomain(firstSegment)
                        )
                        {
                            ctx.Response.Redirect(string.Concat("/https://", path.AsSpan(1)) + ctx.Request.QueryString.Value);
                            return;
                        }
                    }
                    if (firstSegment.StartsWith("@did:"))
                    {
                        var firstSegmentLength = firstSegment.Length;
                        var handle = await apis.TryDidToHandleAsync(firstSegment.Slice(1).ToString(), RequestContext.CreateForRequest("RedirectDidToHandle", TryGetSession(ctx), urgent: true));
                        if (handle != null)
                            ctx.Response.Redirect(string.Concat(string.Concat("/@", handle, path.AsSpan(firstSegmentLength + 1), ctx.Request.QueryString.Value)));
                    }
                }
                await req(ctx);
            });
            app.MapRazorComponents<App>();

            app.UseRouting();
            app.UseCors();
            app.UseAntiforgery();
            app.MapHub<AppViewLiteHub>("/api/live-updates");
            app.MapControllers();

            if (listenToFirehose && !relationships.IsReadOnly)
            {



                await Task.Delay(1000);
                app.Logger.LogInformation("Indexing the firehose to {0}... (press CTRL+C to stop indexing)", relationships.BaseDirectory);

                var firehoses = AppViewLiteConfiguration.GetStringList(AppViewLiteParameter.APPVIEWLITE_FIREHOSES) ??
                    [
                        // Some jetstream firehoses don't behave correctly when resuming from cursor: https://github.com/bluesky-social/jetstream/issues/27
                        "[jetstream-bsky-network]jet:jetstream1.us-east.bsky.network|jet:jetstream2.us-east.bsky.network|jet:jetstream1.us-west.bsky.network|jet:jetstream2.us-west.bsky.network",
                        //"bsky.network"
                    ];

                foreach (var firehose in firehoses)
                {
                    if (firehose == "-") continue;

                    var firehoseUrl = FirehoseUrlWithFallbacks.Parse(firehose);
                    var indexer = new Indexer(apis)
                    {
                        FirehoseUrl = firehoseUrl,
                        VerifyValidForCurrentRelay = did =>
                        {
                            if (apis.DidDocOverrides.GetValue().CustomDidDocs.ContainsKey(did))
                            {
                                throw new Exception($"Ignoring firehose record for {did} because a DID doc override was specified for such DID.");
                            }
                        }
                    };

                    if (firehoseUrl.IsJetStream)
                        indexer.StartListeningToJetstreamFirehose(Indexer.CreateMainFirehoseRetryPolicy()).FireAndForget();
                    else
                        indexer.StartListeningToAtProtoFirehoseRepos(Indexer.CreateMainFirehoseRetryPolicy()).FireAndForget();

                }

                var labelFirehoses = (AppViewLiteConfiguration.GetStringList(AppViewLiteParameter.APPVIEWLITE_LABEL_FIREHOSES)
                    ??
                    //["*"]
                    ["mod.bsky.app/did:plc:ar7c4by46qjdydhdevvrndac"]
                    )
                    .Where(x => x != "-")
                    .ToArray();
                if (labelFirehoses.Contains("*"))
                {

                    var allLabelers = apis.WithRelationshipsLock(rels =>
                    {
                        var cacheSlices = rels.DidDocs.GetCache<WhereSelectCache<Plc, byte, Plc, byte>>("labeler")!.cacheSlices;
                        return cacheSlices
                            .SelectMany(x => x.Cache.Enumerate().Select(x => new { LabelerPlc = x.Key, LabelerDid = rels.GetDid(x.Key), LabelerEndpoint = Encoding.UTF8.GetString(x.Values.AsSmallSpan())! }))
                            .GroupBy(x => x.LabelerPlc)
                            .Select(x => x.Last())
                            .ToArray();
                    }, RequestContext.CreateForFirehose("StartListeningToAllLabelers"));

                    Indexer.RunOnFirehoseProcessingThreadpoolFireAndForget(async () =>
                    {
                        // Approx 600 labelers
                        var delayMsBetweenLaunch = TimeSpan.FromSeconds(60).TotalMilliseconds / Math.Max(allLabelers.Length, 1);
                        foreach (var labelFirehose in allLabelers.GroupBy(x => x.LabelerEndpoint))
                        {
                            var labelerDids = labelFirehose.Select(x => x.LabelerDid).ToArray();
                            LoggableBase.LogInfo("Launching labeler firehose: " + labelFirehose.Key + " for " + string.Join(", ", labelerDids));
                            apis.LaunchLabelerListener(labelerDids, labelFirehose.Key);
                            await Task.Delay((int)delayMsBetweenLaunch);
                        }
                        LoggableBase.Log("All labeler listeners were launched.");
                        return 0;
                    });

                }
                else
                {
                    foreach (var labelFirehose in labelFirehoses
                        .Select(x => x.Split('/'))
                        .Select(x => (Endpoint: "https://" + x[0], Did: x[1]))
                    )
                    {
                        apis.LaunchLabelerListener([labelFirehose.Did], labelFirehose.Endpoint);
                    }
                }

                foreach (var pluggableProtocol in AppViewLite.PluggableProtocols.PluggableProtocol.RegisteredPluggableProtocols)
                {
                    Task.Run(() => pluggableProtocol.DiscoverAsync(relationships.ShutdownRequested)).FireAndForget();
                }

            }


            apis.RunGlobalPeriodicFlushLoopAsync().FireAndForget();

            if ((AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_LISTEN_TO_PLC_DIRECTORY) ?? true) && !relationships.IsReadOnly)
            {
                Task.Run(async () =>
                {

                    var indexer = new Indexer(apis);
                    var bundle = AppViewLiteConfiguration.GetString(AppViewLiteParameter.APPVIEWLITE_PLC_DIRECTORY_BUNDLE);
                    if (bundle != null)
                    {
                        await indexer.InitializePlcDirectoryFromBundleAsync(bundle);
                    }
                    await PluggableProtocol.RetryInfiniteLoopAsync("PlcDirectory", async ct =>
                    {
                        while (true)
                        {
                            await indexer.RetrievePlcDirectoryAsync();

                            await Task.Delay(TimeSpan.FromMinutes(20), ct);
                        }
                    }, default, retryPolicy: RetryPolicy.CreateConstant(TimeSpan.FromMinutes(5)));
                }).FireAndForget();

            }

            AppViewLiteHubContext = app.Services.GetRequiredService<IHubContext<AppViewLiteHub>>();
            RequestContext.SendSignalrImpl = (signalrSessionId, method, args) => AppViewLiteHubContext.Clients.Client(signalrSessionId).SendCoreAsync(method, args);

            LoggableBase.Log("AppViewLite is now serving requests on ========> " + string.Join(", ", bindUrls));
            app.Run();

        }

        private static bool IsKnownCacheablePath(PathString path)
        {
            if (path.StartsWithSegments("/lib")) return true;
            if (path.StartsWithSegments("/img")) return true;
            var p = path.Value;
            if (p != null)
            {
                if (p.StartsWith("/app.", StringComparison.Ordinal) && (p.EndsWith(".css", StringComparison.Ordinal) || p.EndsWith(".js", StringComparison.Ordinal)))
                    return true;
            }
            return false;
        }

        internal static IHubContext<AppViewLiteHub> AppViewLiteHubContext = null!;
        public static IServiceProvider StaticServiceProvider = null!;

        public static AppViewLiteSession? TryGetSession(HttpContext? httpContext)
        {
            return BlueskyEnrichedApis.Instance.TryGetSessionFromCookie(TryGetSessionCookie(httpContext) ?? default);
        }


        public static SessionIdWithUnverifiedDid? TryGetSessionCookie(HttpContext? httpContext)
        {
            if (httpContext == null) return null;
            if (httpContext.Request.Path.StartsWithSegments("/lib")) return null;
            if (httpContext.Request.Path.StartsWithSegments("/xrpc"))
            {
                var authorization = httpContext.Request.Headers.Authorization.FirstOrDefault();
                if (authorization != null && authorization.StartsWith("Bearer ", StringComparison.Ordinal))
                {
                    var handler = new JwtSecurityTokenHandler();
                    var unverifiedJwtToken = authorization.Substring(7).Trim();
                    var parsedUnverifiedJwtToken = handler.ReadJwtToken(unverifiedJwtToken);
                    var unverifiedDid = parsedUnverifiedJwtToken.Subject ?? string.Empty;
                    if (!unverifiedDid.StartsWith("did:", StringComparison.Ordinal))
                    {
                        unverifiedDid = parsedUnverifiedJwtToken.Issuer;
                    }
                    if (!BlueskyEnrichedApis.IsValidDid(unverifiedDid))
                        throw new Exception("A valid DID identifier was not found inside the Subject or Issue field of the JWT token. https://github.com/alnkesq/AppViewLite/pull/215");
                    return new SessionIdWithUnverifiedDid(unverifiedDid, unverifiedJwtToken);
                }
                return null;
            }
            var allSessions = ParsedMultisessionCookie.Parse(httpContext.Request.Cookies[ParsedMultisessionCookie.CookieName]);
            var xhrPreferredDid = httpContext.Request.Headers["X-AppViewLitePreferredDid"].FirstOrDefault();
            if (xhrPreferredDid == null && httpContext.Request.Path.StartsWithSegments("/api/live-updates"))
            {
                xhrPreferredDid = httpContext.Request.Query["activedid"].FirstOrDefault();
            }
            if (!string.IsNullOrEmpty(xhrPreferredDid))
            {
                var session = allSessions.Sessions.FirstOrDefault(x => x.UnverifiedDid == xhrPreferredDid);
                if (session == default) throw new Exception("The session you're trying to use has been logged out. Please refresh the page.");
                return session;
            }
            if (allSessions.ActiveDid == null) return null;
            return allSessions.Sessions.First(x => x.UnverifiedDid == allSessions.ActiveDid);
        }

        public static Uri? GetNextContinuationUrl(this NavigationManager url, string? nextContinuation)
        {
            if (nextContinuation == null) return null;
            return url.WithQueryParameter("continuation", nextContinuation);
        }
        public static Uri WithQueryParameter(this NavigationManager url, string name, string? value)
        {
            return url.ToAbsoluteUri(url.Uri).WithQueryParameter(name, value);
        }

        public static void AppendEssentialCookie(this HttpResponse response, string name, string value)
        {
            response.Cookies.Append(name, value, new CookieOptions { IsEssential = true, MaxAge = TimeSpan.FromDays(3650), SameSite = SameSiteMode.Lax /* with Strict, navigation from external domains would create a cookieless page */, Secure = true });
        }

        public static Uri WithQueryParameter(this Uri url, string name, string? value)
        {
            var query = QueryHelpers.ParseQuery(url.Query);
            if (value != null)
            {
                query[name] = value;
            }
            else
            {
                query.Remove(name);
            }
            return new Uri(QueryHelpers.AddQueryString(url.GetLeftPart(UriPartial.Path), query));
        }

        public static Uri RemoveTrackingParameters(this Uri url)
        {
            var modified = false;
            var query = QueryHelpers.ParseQuery(url.Query);
            foreach (var p in query.ToArray())
            {
                var name = p.Key;
                if (name.StartsWith("utm_", StringComparison.Ordinal))
                {
                    modified = true;
                    query.Remove(name);
                }

            }
            if (modified)
            {
                return new Uri(QueryHelpers.AddQueryString(url.GetLeftPart(UriPartial.Path), query) + url.Fragment);
            }
            return url;
        }
        internal static Task<string> RenderComponentAsync<T>(Dictionary<string, object?> parameters) where T : ComponentBase
        {
            return RenderComponentAsync<T>(ParameterView.FromDictionary(parameters));

        }
        internal static async Task<string> RenderComponentAsync<T>(ParameterView parameters) where T : ComponentBase
        {
            using var scope = Program.StaticServiceProvider.CreateScope();
            using var renderer = new HtmlRenderer(scope.ServiceProvider, scope.ServiceProvider.GetRequiredService<ILoggerFactory>());
            var html = await renderer.Dispatcher.InvokeAsync(async () => (await renderer.RenderComponentAsync<T>(parameters)).ToHtmlString());
            return html;
        }


        public static void RedirectIfNotLoggedIn(this NavigationManager navigation, RequestContext ctx)
        {
            if (ctx.IsLoggedIn) return;
            navigation.NavigateTo("/login?return=" + Uri.EscapeDataString(new Uri(navigation.Uri).PathAndQuery), true);
        }

        public static Results<ATResult<T>, ATErrorResult> ToJsonResultOk<T>(this T result) where T : ATObject
        {
            return ATResult<T>.Ok(result);
        }
        public static Task<Results<ATResult<T>, ATErrorResult>> ToJsonResultOkTask<T>(this T result) where T : ATObject
        {
            return Task.FromResult<Results<ATResult<T>, ATErrorResult>>(ATResult<T>.Ok(result));
        }
        public static Task<Results<Ok, ATErrorResult>> ToJsonResultTask(this Ok result)
        {
            return Task.FromResult<Results<Ok, ATErrorResult>>(result);
        }

        public static Task<Results<ATResult<T>, ATErrorResult>> ToJsonResultTask<T>(this ATErrorResult error) where T : ATObject
        {
            return Task.FromResult<Results<ATResult<T>, ATErrorResult>>(error);
        }

        public static string HashToCssColor(string? table, string? operation) => HashToCssColor(table + "|" + operation);
        public static string HashToCssColor(ReadOnlySpan<char> seed) => HashToCssColor(MemoryMarshal.AsBytes(seed));

        public static string HashToCssColor(ReadOnlySpan<byte> seed)
        {
            var seedInt = (int)System.IO.Hashing.XxHash32.HashToUInt32(seed);
            var rng = new Random(seedInt);

            int hue = rng.Next(0, 360);
            int sat = rng.Next(50, 91);
            int light = rng.Next(60, 80);
            return $"hsl({hue}, {sat}%, {light}%)";
        }
    }
}
