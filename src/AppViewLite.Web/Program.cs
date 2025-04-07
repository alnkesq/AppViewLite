using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.WebUtilities;
using AppViewLite.Web.Components;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Components.Web;
using AppViewLite.PluggableProtocols;
using Microsoft.AspNetCore.Http.Extensions;
using Microsoft.AspNetCore.HttpOverrides;
using AppViewLite.Storage;

namespace AppViewLite.Web
{
    public static class Program
    {

        public static async Task Main(string[] args)
        {
            var apis = AppViewLiteInit.Init(args);
            var relationships = apis.DangerousUnlockedRelationships;
            var listenToFirehose = AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_LISTEN_TO_FIREHOSE) ?? true;
            var builder = WebApplication.CreateBuilder(args);
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
            builder.Services.AddControllers().AddJsonOptions(options =>
            {
                options.JsonSerializerOptions.DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull;
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
                if (request?.Path.StartsWithSegments("/ErrorHttpStatus") == true) { return RequestContext.CreateForRequest(); }
                var signalrConnectionId = request?.Headers["X-AppViewLiteSignalrId"].FirstOrDefault();
                var urgent = request?.Method == "CONNECT" ? false : (request?.Headers["X-AppViewLiteUrgent"].FirstOrDefault() != "0");

                var ctx = RequestContext.CreateForRequest(session, string.IsNullOrEmpty(signalrConnectionId) ? null : signalrConnectionId, urgent: urgent, requestUrl: httpContext?.Request.GetEncodedPathAndQuery());
                return ctx;
            });
            builder.Services.AddSignalR();

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
            app.Lifetime.ApplicationStopping.Register(relationships.NotifyShutdownRequested);



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
                    context.Response.StatusCode = 403;
                    await context.Response.WriteAsync("Cross-site POST requests are not allowed.");
                    return;
                }

                await next();
            });

            app.UseHttpsRedirection();

            app.UseStatusCodePagesWithReExecute("/ErrorHttpStatus", "?code={0}");
            app.UseAntiforgery();


            app.MapStaticAssets();
            app.Use(async (ctx, req) =>
            {

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
                            apis.LogOut(reqCtx.Session.SessionToken!, reqCtx.Session.Did!, reqCtx);
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
                        var handle = await apis.TryDidToHandleAsync(firstSegment.Slice(1).ToString(), RequestContext.CreateForRequest(TryGetSession(ctx), urgent: true));
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
                        "jet:jetstream.atproto.tools",
                        //"bsky.network"
                    ];

                foreach (var firehose in firehoses)
                {
                    bool isJetStream;
                    string firehoseUrl;

                    if (firehose.StartsWith("jet:", StringComparison.Ordinal))
                    {
                        isJetStream = true;
                        firehoseUrl = string.Concat("https://", firehose.AsSpan(4));
                    }
                    else
                    {
                        isJetStream = false;
                        firehoseUrl = "https://" + firehose;
                    }


                    var indexer = new Indexer(apis)
                    {
                        FirehoseUrl = new Uri(firehoseUrl),
                        VerifyValidForCurrentRelay = did =>
                        {
                            if (apis.DidDocOverrides.GetValue().CustomDidDocs.ContainsKey(did))
                            {
                                throw new Exception($"Ignoring firehose record for {did} because a DID doc override was specified for such DID.");
                            }
                        }
                    };

                    if (isJetStream)
                        indexer.StartListeningToJetstreamFirehose().FireAndForget();
                    else
                        indexer.StartListeningToAtProtoFirehoseRepos().FireAndForget();

                }

                var labelFirehoses = (AppViewLiteConfiguration.GetStringList(AppViewLiteParameter.APPVIEWLITE_LABEL_FIREHOSES) ?? ["mod.bsky.app/did:plc:ar7c4by46qjdydhdevvrndac"])
                    .Select(x => x.Split('/'))
                    .Select(x => (Endpoint: "https://" + x[0], Did: x[1]))
                    .ToArray();
                foreach (var labelFirehose in labelFirehoses)
                {
                    var indexer = new Indexer(apis)
                    {
                        FirehoseUrl = new(labelFirehose.Endpoint),
                        VerifyValidForCurrentRelay = did =>
                        {
                            if (did != labelFirehose.Did)
                                throw new Exception($"Ignoring label from {did} because it didn't come from {labelFirehose.Endpoint}");
                        }
                    };
                    indexer.StartListeningToAtProtoFirehoseLabels(labelFirehose.Endpoint).FireAndForget();
                }

                foreach (var pluggableProtocol in AppViewLite.PluggableProtocols.PluggableProtocol.RegisteredPluggableProtocols)
                {
                    Task.Run(() => pluggableProtocol.DiscoverAsync(relationships.ShutdownRequested)).FireAndForget();
                }

            }


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
                    await PluggableProtocol.RetryInfiniteLoopAsync(async ct =>
                    {
                        while (true)
                        {
                            await indexer.RetrievePlcDirectoryAsync();

                            await Task.Delay(TimeSpan.FromMinutes(20), ct);
                        }
                    }, default, TimeSpan.FromMinutes(5));
                }).FireAndForget();

            }

            AppViewLiteHubContext = app.Services.GetRequiredService<IHubContext<AppViewLiteHub>>();
            RequestContext.SendSignalrImpl = (signalrSessionId, method, args) => AppViewLiteHubContext.Clients.Client(signalrSessionId).SendCoreAsync(method, args);
            app.Run();

        }



        internal static IHubContext<AppViewLiteHub> AppViewLiteHubContext = null!;
        public static IServiceProvider StaticServiceProvider = null!;

        public static AppViewLiteSession? TryGetSession(HttpContext? httpContext)
        {
            return BlueskyEnrichedApis.Instance.TryGetSessionFromCookie(TryGetSessionCookie(httpContext));
        }

        public static string? TryGetSessionCookie(HttpContext? httpContext)
        {
            return httpContext != null && httpContext.Request.Cookies.TryGetValue("appviewliteSessionId", out var id) && !string.IsNullOrEmpty(id) ? id : null;
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

    }
}
