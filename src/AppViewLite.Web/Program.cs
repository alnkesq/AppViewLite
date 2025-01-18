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

            var devSession = false ? CreateDevSession() : null;
            
            builder.Services.AddScoped(provider =>
            {
                if (devSession != null) return devSession;

                var httpContext = provider.GetRequiredService<IHttpContextAccessor>().HttpContext!;

                return TryGetSession(httpContext) ?? new();
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

        private static AppViewLiteSession CreateDevSession()
        {
            var testDid = "did:plc:yrzav4kckt5na2uzgx3j3s2r";
            var testSession = new AppViewLiteSession
            {
                LoggedInUser = BlueskyEnrichedApis.Instance.WithRelationshipsLock(rels => rels.SerializeDid(testDid)),
                Profile = BlueskyEnrichedApis.Instance.GetProfileAsync(testDid, EnrichDeadlineToken.Infinite).Result
            };
            return testSession;
        }

        public static AppViewLiteSession? TryGetSession(HttpContext httpContext)
        {
            var id = TryGetSessionId(httpContext);
            if (id != null && SessionDictionary.TryGetValue(id, out var session))
            {
                session.LastSeen = DateTime.UtcNow;
                return session;
            }
            return null;
        }

        public static async Task<AppViewLiteSession> LogInAsync(HttpContext httpContext, string handle)
        {
            var did = await BlueskyEnrichedApis.Instance.ResolveHandleAsync(handle);
            var id = RandomNumberGenerator.GetHexString(32, lowercase: true);
            httpContext.Response.Cookies.Append("appviewliteSessionId", id, new CookieOptions { IsEssential = true, MaxAge = TimeSpan.FromDays(3650), SameSite = SameSiteMode.Strict });
            var session = new AppViewLiteSession();
            session.LastSeen = DateTime.UtcNow;
            session.LoggedInUser = BlueskyEnrichedApis.Instance.WithRelationshipsLock(rels => rels.SerializeDid(did));
            session.Profile = await BlueskyEnrichedApis.Instance.GetProfileAsync(did, EnrichDeadlineToken.Infinite);
            SessionDictionary[id] = session;
            
            return session;
        }

        public static void LogOut(HttpContext httpContext)
        {
            var id = TryGetSessionId(httpContext);
            if (id != null)
            {
                SessionDictionary.Remove(id, out _);
            }

        }
        public static ConcurrentDictionary<string, AppViewLiteSession> SessionDictionary = new();

        private static string? TryGetSessionId(HttpContext httpContext)
        {
            if (httpContext.Request.Cookies.TryGetValue("appviewliteSessionId", out var id) && !string.IsNullOrEmpty(id))
                return id;
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

