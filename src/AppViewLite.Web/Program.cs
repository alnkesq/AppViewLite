using AppViewLite.Web.Components;
using AppViewLite;
using FishyFlip;
using FishyFlip.Models;
using Microsoft.AspNetCore.Mvc.ApiExplorer;
using AppViewLite.Storage;
using Ipfs;
using System.Text.Json.Serialization;
using System.Security.Cryptography;
using Microsoft.AspNetCore.Http;
using System.Collections.Concurrent;
using AppViewLite.Models;

namespace AppViewLite.Web
{
    public class Program
    {
        protected static BlueskyRelationships Relationships;


        public static string ToFullHumanDate(DateTime date)
        {
            return date.ToString("MMM d, yyyy HH:mm");
        }
        public static string ToHumanDate(DateTime date)
        {
            var ago = DateTime.UtcNow - date;
            if (ago.TotalHours < 1) return (int)Math.Max(ago.TotalMinutes, 1) + "m";
            if (ago.TotalDays < 1) return (int)ago.TotalHours + "h";
            if (date.Year == DateTime.UtcNow.Year) return date.ToString("MMM d");
            return date.ToString("MMM d, yyyy");
        }

        public static string GetImageThumbnailUrl(string did, byte[] cid)
        {
            return $"https://cdn.bsky.app/img/feed_thumbnail/plain/{did}/{ Cid.Read(cid) }@jpeg";
        }
        public static string GetImageFullUrl(string did, byte[] cid)
        {
            return $"https://cdn.bsky.app/img/feed_fullsize/plain/{did}/{Cid.Read(cid) }@jpeg";
        }

        public static async Task Main(string[] args)
        {
            Relationships = new();
            BlueskyEnrichedApis.Instance = new(Relationships);
            var builder = WebApplication.CreateBuilder(args);
            // Add services to the container.
            builder.Services.AddRazorComponents()
                .AddInteractiveServerComponents()
                .AddInteractiveWebAssemblyComponents();

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
                var httpContext = provider.GetRequiredService<IHttpContextAccessor>().HttpContext!;

                return TryGetSession(httpContext) ?? new();
            });
            builder.Services.AddScoped(provider =>
            {
                var session = provider.GetRequiredService<AppViewLiteSession>();
                return RequestContext.Create(session);
            });

            var app = builder.Build();

            app.Lifetime.ApplicationStopping.Register(Relationships.Dispose);
            var indexer = new Indexer(Relationships);


            // Configure the HTTP request pipeline.
            if (app.Environment.IsDevelopment())
            {
                app.UseWebAssemblyDebugging();
            }
            else
            {
                app.UseExceptionHandler("/Error");
                // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
                app.UseHsts();
            }

            app.UseHttpsRedirection();

            app.UseAntiforgery();

            

            app.MapStaticAssets();
            app.MapRazorComponents<App>()
                .AddInteractiveServerRenderMode()
                .AddInteractiveWebAssemblyRenderMode();

            app.UseRouting();
            app.UseCors();
            app.UseAntiforgery();
            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            });

            _ = Task.Run(async () =>
            {
                await Task.Delay(1000);
                app.Logger.LogInformation("Indexing the firehose to {0}... (press CTRL+C to stop indexing)", Relationships.BaseDirectory);
                await indexer.ListenJetStreamFirehoseAsync();
            });
            app.Run();
            
        }

        public static AppViewLiteSession? TryGetSession(HttpContext httpContext)
        {
            var now = DateTime.UtcNow;
            var sessionId = TryGetSessionId(httpContext, out var unverifiedDid);
            if (sessionId != null)
            {
                if (!SessionDictionary.TryGetValue(sessionId, out var session))
                {
                    AppViewLiteProfileProto? profile = null;
                    AppViewLiteSessionProto? sessionProto = null;
                    Plc plc = default;
                    string? did = null;
                    BlueskyProfile? bskyProfile = null;

                    BlueskyEnrichedApis.Instance.WithRelationshipsLock(rels =>
                    {
                        var unverifiedPlc = rels.SerializeDid(unverifiedDid!);
                        var unverifiedProfile = rels.TryGetAppViewLiteProfile(unverifiedPlc);
                        sessionProto = unverifiedProfile?.Sessions?.FirstOrDefault(x => x.SessionToken == sessionId);
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
                        LoggedInUser = plc,
                        LastSeen = now,
                        Profile = bskyProfile, // TryGetSession cannot be async. Prepare a preliminary profile if not loaded yet.
                    };
                    _ = OnSessionCreatedOrRestoredAsync(did!, plc, session);
                    SessionDictionary[sessionId] = session;
                }

                session.LastSeen = now;
                return session;
            }

            return null;
        }

        public static async Task<AppViewLiteSession> LogInAsync(HttpContext httpContext, string handle)
        {
            var did = await BlueskyEnrichedApis.Instance.ResolveHandleAsync(handle);
            var id = RandomNumberGenerator.GetHexString(32, lowercase: true);
            httpContext.Response.Cookies.Append("appviewliteSessionId", id + "=" + did, new CookieOptions { IsEssential = true, MaxAge = TimeSpan.FromDays(3650), SameSite = SameSiteMode.Strict });
            var now = DateTime.UtcNow;
            var session = new AppViewLiteSession();
            session.LastSeen = now;

            Plc plc = default;
            BlueskyEnrichedApis.Instance.WithRelationshipsLock(rels => 
            {
                plc = rels.SerializeDid(did);
                session.LoggedInUser = plc;
                rels.RegisterForNotifications(session.LoggedInUser.Value);
                var profile = rels.TryGetAppViewLiteProfile(plc) ?? new AppViewLiteProfileProto { FirstLogin = now };
                profile!.Sessions ??= new();
                profile.Sessions.Add(new AppViewLiteSessionProto
                {
                    LastSeen = now,
                    SessionToken = id
                });
                rels.StoreAppViewLiteProfile(plc, profile);
            });

            await OnSessionCreatedOrRestoredAsync(did, plc, session);

            SessionDictionary[id] = session;
            
            return session;
        }

        private static async Task OnSessionCreatedOrRestoredAsync(string did, Plc plc, AppViewLiteSession session)
        {


            var haveFollowees = BlueskyEnrichedApis.Instance.WithRelationshipsLock(rels => rels.RegisteredUserToFollowees.GetValueCount(plc));
            session.Profile = await BlueskyEnrichedApis.Instance.GetProfileAsync(did, RequestContext.CreateInfinite(null));

            if (haveFollowees < 100)
            {
                var deadline = Task.Delay(5000);
                var loadFollows = BlueskyEnrichedApis.Instance.ImportCarIncrementalAsync(did, Models.RepositoryImportKind.Follows, ignoreIfRecentlyRan: TimeSpan.FromDays(90));
                _ = BlueskyEnrichedApis.Instance.ImportCarIncrementalAsync(did, Models.RepositoryImportKind.Blocks, ignoreIfRecentlyRan: TimeSpan.FromDays(90));
                _ = BlueskyEnrichedApis.Instance.ImportCarIncrementalAsync(did, Models.RepositoryImportKind.BlocklistSubscriptions, ignoreIfRecentlyRan: TimeSpan.FromDays(90));
                // TODO: fetch entries of subscribed blocklists
                await Task.WhenAny(deadline, loadFollows);
            }

        }

        public static void LogOut(HttpContext httpContext)
        {
            var id = TryGetSessionId(httpContext, out var unverifiedDid);
            if (id != null)
            {
                SessionDictionary.Remove(id, out _);

                BlueskyEnrichedApis.Instance.WithRelationshipsLock(rels =>
                {
                    var unverifiedPlc = rels.SerializeDid(unverifiedDid!);
                    var unverifiedProfile = rels.TryGetAppViewLiteProfile(unverifiedPlc);
                    var session = unverifiedProfile?.Sessions?.FirstOrDefault(x => x.SessionToken == id);
                    if (session != null)
                    {
                        // now verified.
                        unverifiedProfile!.Sessions.Remove(session);
                        rels.StoreAppViewLiteProfile(unverifiedPlc, unverifiedProfile); 
                    }
                });
            }

        }
        public static ConcurrentDictionary<string, AppViewLiteSession> SessionDictionary = new();

        private static string? TryGetSessionId(HttpContext httpContext, out string? unverifiedDid)
        {
            if (httpContext.Request.Cookies.TryGetValue("appviewliteSessionId", out var id) && !string.IsNullOrEmpty(id))
            {
                var r = id.Split('=');
                unverifiedDid = r[1];
                return r[0];
            }
            unverifiedDid = null;
            return null;
        }

        public static async Task<ATUri> ResolveUriAsync(string uri)
        {
            var aturi = new ATUri(uri);
            if (aturi.Did != null) return aturi;
            
            var did = await BlueskyEnrichedApis.Instance.ResolveHandleAsync(aturi.Handle!.Handle);
            return new ATUri("at://" + did + aturi.Pathname + aturi.Hash);
        }
    }
}

