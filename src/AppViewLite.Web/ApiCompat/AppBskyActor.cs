using AppViewLite.Models;
using FishyFlip.Lexicon;
using FishyFlip.Lexicon.App.Bsky.Actor;
using FishyFlip.Models;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;

#pragma warning disable CS1998 // Unnecessary async

namespace AppViewLite.Web
{
    [ApiController]
    [EnableCors("BskyClient")]
    public class AppBskyActor : FishyFlip.Xrpc.Lexicon.App.Bsky.Actor.ActorController
    {
        private readonly BlueskyEnrichedApis apis;
        private readonly RequestContext ctx;
        public AppBskyActor(BlueskyEnrichedApis apis, RequestContext ctx)
        {
            this.apis = apis;
            this.ctx = ctx;
        }

        /// <inheritdoc/>
        public override async Task<Results<Ok<GetPreferencesOutput>, ATErrorResult>> GetPreferencesAsync(CancellationToken cancellationToken = default)
        {
            return TypedResults.Ok(new GetPreferencesOutput
            {
                Preferences = [
                    new SavedFeedsPrefV2 { Items = [new SavedFeed("3lemacgq3ne2v", "timeline", "following", pinned: true)] },
                    new AdultContentPref { Enabled = true }
                ]
            });
        }

        /// <inheritdoc/>
        public async override Task<Results<Ok<ProfileViewDetailed>, ATErrorResult>> GetProfileAsync([FromQuery] ATIdentifier actor, CancellationToken cancellationToken = default)
        {
            // TODO: Can be a handle or a did.
            var profile = await apis.GetFullProfileAsync(((ATDid)actor).ToString(), ctx, 0);

            return TypedResults.Ok(profile.ToApiCompatProfileDetailed());
        }

        /// <inheritdoc/>
        public async override Task<Results<Ok<GetProfilesOutput>, ATErrorResult>> GetProfilesAsync([FromQuery] List<ATIdentifier> actors, CancellationToken cancellationToken = default)
        {
            // TODO: Actors can be a handles or dids and should handle both.
            if (actors.Count == 0) return TypedResults.Ok(new GetProfilesOutput { Profiles = [] });

            // TODO: where is getProfiles used?

            if (actors.Count == 1) return TypedResults.Ok(new GetProfilesOutput { Profiles = [ApiCompatUtils.ToApiCompatProfileDetailed(await apis.GetFullProfileAsync(((ATDid)actors[0]).ToString(), ctx, 0))] });

            var actorsStr = actors.Select(x => ((ATDid)x).ToString()).ToArray();
            var profiles = apis.WithRelationshipsLockForDids(actorsStr, (plcs, rels) =>
            {
                return plcs.Select(x =>
                {
                    var p = rels.GetProfile(x, ctx);
                    return new BlueskyFullProfile
                    {
                        Profile = p,
                    };
                }).ToArray();
            }, ctx);
            await apis.EnrichAsync(profiles.Select(x => x.Profile).ToArray(), ctx);
            return TypedResults.Ok(new GetProfilesOutput
            {
                Profiles = profiles.Select(x => ApiCompatUtils.ToApiCompatProfileDetailed(x)).ToList(),
            });
        }

        /// <inheritdoc/>
        public async override Task<Results<Ok<GetSuggestionsOutput>, ATErrorResult>> GetSuggestionsAsync([FromQuery] int? limit = 50, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            return TypedResults.Ok(new GetSuggestionsOutput
            {
                Actors = []
            });
        }

        /// <inheritdoc/>
        public async override Task<Results<Ok, ATErrorResult>> PutPreferencesAsync([FromBody] List<ATObject> preferences, CancellationToken cancellationToken = default)
        {
            return TypedResults.Ok();
        }

        /// <inheritdoc/>
        public async override Task<Results<Ok<SearchActorsOutput>, ATErrorResult>> SearchActorsAsync([FromQuery] string? q = null, [FromQuery] int? limit = 25, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            var results = await apis.SearchProfilesAsync(q, allowPrefixForLastWord: false, cursor, limit ?? 25, ctx);
            return TypedResults.Ok(new SearchActorsOutput
            {
                Actors = results.Profiles.Select(x => ApiCompatUtils.ToApiCompatProfileView(x)).ToList(),
                Cursor = results.NextContinuation
            });
        }

        /// <inheritdoc/>
        public async override Task<Results<Ok<SearchActorsTypeaheadOutput>, ATErrorResult>> SearchActorsTypeaheadAsync([FromQuery] string? q = null, [FromQuery] int? limit = 10, CancellationToken cancellationToken = default)
        {
            var results = await apis.SearchProfilesAsync(q, allowPrefixForLastWord: true, null, limit ?? 10, ctx);
            return TypedResults.Ok(new SearchActorsTypeaheadOutput
            {
                Actors = results.Profiles.Select(x => ApiCompatUtils.ToApiCompatProfileViewBasic(x)).ToList(),
            });
        }
    }
}

