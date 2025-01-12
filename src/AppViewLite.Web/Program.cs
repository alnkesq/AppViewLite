using AppViewLite.Web.Components;
using AppViewLite;
using FishyFlip;
using FishyFlip.Models;
using Microsoft.AspNetCore.Mvc.ApiExplorer;
using AppViewLite.Storage;
using Ipfs;

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

        public static T WithRelationshipsLock<T>(Func<BlueskyRelationships, T> func)
        {
            BlueskyRelationships.VerifyNotEnumerable<T>();
            lock (Relationships)
            {
                return func(Relationships);
            }
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

            _ = Task.Run(async () =>
            {
                await Task.Delay(1000);
                app.Logger.LogInformation("Indexing the firehose to {0}... (press CTRL+C to stop indexing)", Relationships.BaseDirectory);
                return indexer.ListenJetStreamFirehoseAsync();
            });
            app.Run();
            
        }
    }
}

