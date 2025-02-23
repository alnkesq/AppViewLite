using FishyFlip.Models;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.WebUtilities;
using AppViewLite.Web.Components;
using AppViewLite.Models;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Components.Web;
using System.Text.RegularExpressions;
using AppViewLite.PluggableProtocols;

namespace AppViewLite.Web
{
    public static class Program
    {
        private static BlueskyRelationships Relationships = null!;

        public static bool AllowPublicReadOnlyFakeLogin = AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_ALLOW_PUBLIC_READONLY_FAKE_LOGIN) ?? false;
        public static bool ListenToFirehose = AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_LISTEN_TO_FIREHOSE) ?? true;



        public static async Task Main(string[] args)
        {
            Relationships = new();
            var apis = new BlueskyEnrichedApis(Relationships);
            apis.RegisterPluggableProtocol(typeof(AppViewLite.PluggableProtocols.ActivityPub.ActivityPubProtocol));
            apis.RegisterPluggableProtocol(typeof(AppViewLite.PluggableProtocols.Nostr.NostrProtocol));
            apis.RegisterPluggableProtocol(typeof(AppViewLite.PluggableProtocols.Yotsuba.YotsubaProtocol));
            apis.RegisterPluggableProtocol(typeof(AppViewLite.PluggableProtocols.HackerNews.HackerNewsProtocol));
            apis.RegisterPluggableProtocol(typeof(AppViewLite.PluggableProtocols.Rss.RssProtocol)); // lowest priority for TryGetDidOrLocalPathFromUrlAsync

            BlueskyEnrichedApis.Instance = apis;
            var builder = WebApplication.CreateBuilder(args);
            // Add services to the container.
            builder.Services.AddRazorComponents();
            builder.Services.AddRazorPages();
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
                return TryGetSession(httpContext) ?? new() { IsReadOnlySimulation = true };
            });
            builder.Services.AddScoped(provider =>
            {
                var session = provider.GetRequiredService<AppViewLiteSession>();
                var httpContext = provider.GetRequiredService<IHttpContextAccessor>().HttpContext;
                var signalrConnectionId = httpContext?.Request.Headers["X-AppViewLiteSignalrId"].FirstOrDefault();
                var urgent = httpContext?.Request.Headers["X-AppViewLiteUrgent"].FirstOrDefault() != "0";
                return RequestContext.Create(session, string.IsNullOrEmpty(signalrConnectionId) ? null : signalrConnectionId, urgent: urgent);
            });
            builder.Services.AddSignalR();

            var app = builder.Build();

            StaticServiceProvider = app.Services;
            app.Lifetime.ApplicationStopping.Register(Relationships.NotifyShutdownRequested);



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

            app.UseHttpsRedirection();

            app.UseStatusCodePagesWithReExecute("/ErrorHttpStatus", "?code={0}");
            app.UseAntiforgery();

            
            app.MapStaticAssets();
            app.Use(async (ctx, req) => 
            {
                var path = ctx.Request.Path.Value?.ToString();
                if (path != null)
                {
                    var s = path.AsSpan(1);
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
                            !IsKnownFileTypeAsOpposedToTld(firstSegment.Slice(firstSegment.LastIndexOf('.') + 1)) &&
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
                        var handle = await apis.TryDidToHandleAsync(firstSegment.Slice(1).ToString(), RequestContext.Create(TryGetSession(ctx), urgent: true));
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

            if (ListenToFirehose && !Relationships.IsReadOnly)
            {



                await Task.Delay(1000);
                app.Logger.LogInformation("Indexing the firehose to {0}... (press CTRL+C to stop indexing)", Relationships.BaseDirectory);

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
                    Task.Run(() => pluggableProtocol.DiscoverAsync(Relationships.ShutdownRequested)).FireAndForget();
                }

            }


            if ((AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_LISTEN_TO_PLC_DIRECTORY) ?? true) && !Relationships.IsReadOnly)
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

        private static bool IsKnownFileTypeAsOpposedToTld(ReadOnlySpan<char> ext)
        {
            return
                ext.SequenceEqual("js") ||
                ext.SequenceEqual("css") ||
                ext.SequenceEqual("txt") ||
                ext.SequenceEqual("html") ||
                ext.SequenceEqual("gif") ||
                ext.SequenceEqual("jpg") ||
                ext.SequenceEqual("png") ||
                ext.SequenceEqual("xml") ||
                ext.SequenceEqual("ico");
        }

        internal static IHubContext<AppViewLiteHub> AppViewLiteHubContext = null!;
        public static AppViewLiteSession? TryGetSession(HttpContext? httpContext)
        {
            return TryGetSessionFromCookie(TryGetSessionCookie(httpContext));
        }
        public static AppViewLiteSession? TryGetSessionFromCookie(string? sessionIdCookie)
        {
            if (sessionIdCookie == null) return null;
            var apis = BlueskyEnrichedApis.Instance;
            var now = DateTime.UtcNow;
            var sessionId = TryGetSessionIdFromCookie(sessionIdCookie, out var unverifiedDid);
            if (sessionId != null)
            {
                if (!SessionDictionary.TryGetValue(sessionId, out var session))
                {
                    AppViewLiteProfileProto? profile = null;
                    AppViewLiteSessionProto? sessionProto = null;
                    Plc plc = default;
                    string? did = null;
                    BlueskyProfile? bskyProfile = null;

                    apis.WithRelationshipsLockForDid(unverifiedDid!, (unverifiedPlc, rels) =>
                    {
                        var unverifiedProfile = rels.TryGetAppViewLiteProfile(unverifiedPlc);
                        sessionProto = TryGetAppViewLiteSession(unverifiedProfile, sessionId);
                        if (sessionProto != null)
                        {
                            profile = unverifiedProfile;
                            plc = unverifiedPlc;
                            did = unverifiedDid;
                            bskyProfile = rels.GetProfile(plc);
                        }
                    });
                    if (profile == null) return null;

                    session = new AppViewLiteSession
                    {
                        IsReadOnlySimulation = sessionProto!.IsReadOnlySimulation,
                        PdsSession = sessionProto.IsReadOnlySimulation ? null : BlueskyEnrichedApis.DeserializeAuthSession(profile.PdsSessionCbor).Session,
                        LoggedInUser = plc,
                        LastSeen = now,
                        Profile = bskyProfile, // TryGetSession cannot be async. Prepare a preliminary profile if not loaded yet.
                    };
                    OnSessionCreatedOrRestoredAsync(did!, plc, session, profile).FireAndForget();
                    SessionDictionary[sessionId] = session;
                }

                session.LastSeen = now;

                return session;
            }

            return null;
        }

        private static AppViewLiteSessionProto? TryGetAppViewLiteSession(AppViewLiteProfileProto? unverifiedProfile, string sessionId)
        {
            return unverifiedProfile?.Sessions?.FirstOrDefault(x => CryptographicOperations.FixedTimeEquals(MemoryMarshal.AsBytes<char>(x.SessionToken), MemoryMarshal.AsBytes<char>(sessionId)));
        }


        public static async Task<AppViewLiteSession> LogInAsync(HttpContext httpContext, string handle, string password)
        {
            var apis = BlueskyEnrichedApis.Instance;
            if (string.IsNullOrEmpty(handle) || string.IsNullOrEmpty(password)) throw new ArgumentException();

            var isReadOnly = AllowPublicReadOnlyFakeLogin ? password == "readonly" : false;

            var did = await apis.ResolveHandleAsync(handle);
            var atSession = isReadOnly ? null : await apis.LoginToPdsAsync(did, password);



            var id = RandomNumberGenerator.GetHexString(32, lowercase: true);
            httpContext.Response.Cookies.Append("appviewliteSessionId", id + "=" + did, new CookieOptions { IsEssential = true, MaxAge = TimeSpan.FromDays(3650), SameSite = SameSiteMode.Strict });
            var now = DateTime.UtcNow;
            var session = new AppViewLiteSession
            {
                LastSeen = now,
                IsReadOnlySimulation = isReadOnly
            };

            Plc plc = default;
            AppViewLiteProfileProto privateProfile = null!;
            apis.WithRelationshipsWriteLock(rels => 
            {
                plc = rels.SerializeDid(did);
                session.LoggedInUser = plc;
                rels.RegisterForNotifications(session.LoggedInUser.Value);
                privateProfile = rels.TryGetAppViewLiteProfile(plc) ?? new AppViewLiteProfileProto { FirstLogin = now };
                privateProfile!.Sessions ??= new();

                if (!isReadOnly)
                {
                    privateProfile.PdsSessionCbor = BlueskyEnrichedApis.SerializeAuthSession(new AuthSession(atSession));
                    session.PdsSession = atSession;
                }

                privateProfile.Sessions.Add(new AppViewLiteSessionProto
                {
                    LastSeen = now,
                    SessionToken = id,
                    IsReadOnlySimulation = isReadOnly,
                });
                rels.StoreAppViewLiteProfile(plc, privateProfile);
            });

            await OnSessionCreatedOrRestoredAsync(did, plc, session, privateProfile);

            SessionDictionary[id] = session;
            
            return session;
        }

        private static async Task OnSessionCreatedOrRestoredAsync(string did, Plc plc, AppViewLiteSession session, AppViewLiteProfileProto privateProfile)
        {
            var apis = BlueskyEnrichedApis.Instance;

            var haveFollowees = apis.WithRelationshipsLock(rels => rels.RegisteredUserToFollowees.GetValueCount(plc));
            session.Profile = await apis.GetProfileAsync(did, RequestContext.CreateInfinite(null));

            session.PrivateProfile = privateProfile;
            session.PrivateFollows = (privateProfile.PrivateFollows ?? []).ToDictionary(x => new Plc(x.Plc), x => x);

            if (haveFollowees < 100)
            {
                var deadline = Task.Delay(5000);
                var loadFollows = apis.ImportCarIncrementalAsync(did, Models.RepositoryImportKind.Follows, ignoreIfRecentlyRan: TimeSpan.FromDays(90));
                apis.ImportCarIncrementalAsync(did, Models.RepositoryImportKind.Blocks, ignoreIfRecentlyRan: TimeSpan.FromDays(90)).FireAndForget();
                apis.ImportCarIncrementalAsync(did, Models.RepositoryImportKind.BlocklistSubscriptions, ignoreIfRecentlyRan: TimeSpan.FromDays(90)).FireAndForget();
                // TODO: fetch entries of subscribed blocklists
                await Task.WhenAny(deadline, loadFollows);
            }

        }

        public static void LogOut(HttpContext httpContext)
        {
            var id = TryGetSessionIdFromCookie(TryGetSessionCookie(httpContext), out var unverifiedDid);
            if (id != null)
            {
                SessionDictionary.Remove(id, out var unverifiedAppViewLiteSession);

                BlueskyEnrichedApis.Instance.WithRelationshipsWriteLock(rels =>
                {
                    var unverifiedPlc = rels.SerializeDid(unverifiedDid!);
                    var unverifiedProfile = rels.TryGetAppViewLiteProfile(unverifiedPlc);
                    var session = TryGetAppViewLiteSession(unverifiedProfile, id);
                    if (session != null)
                    {
                        // now verified.
                        if (unverifiedAppViewLiteSession != null)
                            unverifiedAppViewLiteSession.PdsSession = null;
                        unverifiedProfile!.PdsSessionCbor = null;
                        unverifiedProfile!.Sessions?.Clear();
                        rels.StoreAppViewLiteProfile(unverifiedPlc, unverifiedProfile); 
                    }
                });
            }

        }
        public static ConcurrentDictionary<string, AppViewLiteSession> SessionDictionary = new();
        public static IServiceProvider StaticServiceProvider = null!;

        private static string? TryGetSessionIdFromCookie(string? cookie, out string? unverifiedDid)
        {
            if (cookie != null)
            {
                var r = cookie.Split('=');
                unverifiedDid = r[1];
                return r[0];
            }
            unverifiedDid = null;
            return null;
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
        internal static async Task<string> RenderComponentAsync<T>(ParameterView parameters) where T: ComponentBase
        {
            using var scope = Program.StaticServiceProvider.CreateScope();
            using var renderer = new HtmlRenderer(scope.ServiceProvider, scope.ServiceProvider.GetRequiredService<ILoggerFactory>());
            var html = await renderer.Dispatcher.InvokeAsync(async () => (await renderer.RenderComponentAsync<T>(parameters)).ToHtmlString());
            return html;
        }
        public static string GetExceptionDisplayText(Exception exception)
        {
            if (exception is HttpRequestException ex)
            {
                if (ex.HttpRequestError != default)
                    return $"Could not fetch the resource: {ex.HttpRequestError}";
                if (ex.StatusCode != null)
                    return $"Could not fetch the resource: HTTP {(int)ex.StatusCode} {ex.StatusCode}";
            }
            var message = exception.Message;
            if (string.IsNullOrEmpty(message))
            {
                message = "Error: " + exception.GetType().Name;
            }
            return message;
        }

        public async static Task<string> ResolveUrlAsync(Uri url, Uri baseUrl)
        {
            // bsky.app links
            if (url.Host == "bsky.app")
                return url.PathAndQuery;

            // recursive appviewlite links
            if (url.Host == baseUrl.Host)
                return url.PathAndQuery;

            // atproto profile from custom domain
            var apis = BlueskyEnrichedApis.Instance;
            if (url.PathAndQuery == "/")
            {
                var handle = url.Host;
                try
                {
                    var did = await apis.ResolveHandleAsync(handle);
                    return "/@" + handle;
                }
                catch
                {
                }
            }

            foreach (var protocol in PluggableProtocol.RegisteredPluggableProtocols)
            {
                var did = await protocol.TryGetDidOrLocalPathFromUrlAsync(url);
                if (did != null)
                    return did.StartsWith("did:", StringComparison.Ordinal) ? "/@" + did : did;
            }


            throw new UnexpectedFirehoseDataException("No RSS feeds were found at the specified page.");
        }


    }
}

