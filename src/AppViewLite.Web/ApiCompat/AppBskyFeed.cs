using FishyFlip.Lexicon;
using FishyFlip.Lexicon.App.Bsky.Feed;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using AppViewLite.Models;
using AppViewLite;

namespace AppViewLite.Web.ApiCompat
{
    [Route("/xrpc")]
    [ApiController]
    [EnableCors("BskyClient")]
    public class AppBskyFeed : ControllerBase
    {
        [HttpGet("app.bsky.feed.getPostThread")]
        public async Task<GetPostThreadOutput> GetPostThread(string uri, int depth)
        {

            var aturi = await Program.ResolveUriAsync(uri);
            var thread = await BlueskyEnrichedApis.Instance.GetPostThreadAsync(aturi.Did!.Handler, aturi.Rkey, EnrichDeadlineToken.Create());

            var focalPostIndex = thread.FindIndex(x => x.Did == aturi.Did.Handler && x.RKey == aturi.Rkey);
            if (focalPostIndex == -1) throw new Exception();
            var rootPost = thread[0];

            ThreadViewPost? tvp = null;
            for (int i = 0; i <= focalPostIndex; i++)
            {
                tvp = thread[i].ToApiCompatThreadViewPost(rootPost, tvp);
            }
            
            tvp!.Replies = thread.Skip(focalPostIndex + 1).Select(x => (ATObject)x.ToApiCompatThreadViewPost(rootPost)).ToList();
            return new GetPostThreadOutput
            {
                Thread = tvp,
            };
        }

        [HttpGet("app.bsky.feed.getLikes")]
        public async Task<GetLikesOutput> GetLikes(string uri, string? cursor)
        {
            var aturi = await Program.ResolveUriAsync(uri);
            var likers = await BlueskyEnrichedApis.Instance.GetPostLikersAsync(aturi.Did!.Handler, aturi.Rkey, cursor, default, EnrichDeadlineToken.Create());
            return new GetLikesOutput
            {
                Uri = aturi,
                Cursor = likers.NextContinuation,
                Likes = likers.Profiles.Select(x => new LikeDef { Actor = x.ToApiCompatProfile(), CreatedAt = x.RelationshipRKey!.Value.Date, IndexedAt = x.RelationshipRKey.Value.Date }).ToList(),
            };
        }

        [HttpGet("app.bsky.feed.getRepostedBy")]
        public async Task<GetRepostedByOutput> GetRepostedBy(string uri, string? cursor)
        {
            var aturi = await Program.ResolveUriAsync(uri);
            var reposters = await BlueskyEnrichedApis.Instance.GetPostRepostersAsync(aturi.Did!.Handler, aturi.Rkey, cursor, default, EnrichDeadlineToken.Create());
            return new GetRepostedByOutput
            {
                Uri = aturi,
                Cursor = reposters.NextContinuation,
                RepostedBy = reposters.Profiles.Select(x => x.ToApiCompatProfile()).ToList(),
            };
        }
    }
}

