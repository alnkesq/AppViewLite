using FishyFlip.Lexicon;
using FishyFlip.Lexicon.App.Bsky.Feed;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using AppViewLite.Models;
using AppViewLite;
using System.Linq;

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

            var focalPostIndex = thread.ToList().FindIndex(x => x.Did == aturi.Did.Handler && x.RKey == aturi.Rkey);
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

        [HttpGet("app.bsky.feed.getAuthorFeed")]
        public async Task<GetAuthorFeedOutput> GetAuthorFeed(string actor, GetUserPostsFilter filter, string includePins, int limit, string? cursor)
        {
            if (filter == GetUserPostsFilter.None) filter = GetUserPostsFilter.posts_no_replies;

            if (filter == GetUserPostsFilter.posts_and_author_threads) filter = GetUserPostsFilter.posts_no_replies; // TODO: https://github.com/alnkesq/AppViewLite/issues/16
            var deadline = EnrichDeadlineToken.Create();

            var (posts, nextContinuation) = await BlueskyEnrichedApis.Instance.GetUserPostsAsync(actor,
                includePosts: true, 
                includeReplies: filter != GetUserPostsFilter.posts_no_replies,
                includeReposts: filter == GetUserPostsFilter.posts_no_replies,
                includeLikes: false,
                mediaOnly: filter == GetUserPostsFilter.posts_with_media,
                cursor,
                deadline);

            BlueskyEnrichedApis.Instance.WithRelationshipsLock(rels =>
            {
                foreach (var post in posts)
                {
                    if (post.IsReply)
                    {
                        post.InReplyToFullPost = rels.GetPost(post.InReplyToPostId!.Value);
                        //post.RootFullPost = rels.GetPost(post.Data.InReplyToRKey); // TODO: https://github.com/alnkesq/AppViewLite/issues/15
                    }
                }
            });
            await BlueskyEnrichedApis.Instance.EnrichAsync([..posts.Select(x => x.InReplyToFullPost).Where(x => x != null)!, ..posts.Select(x => x.RootFullPost).Where(x => x != null)!], deadline);
            return new GetAuthorFeedOutput
            {
                 Cursor = nextContinuation,
                 Feed = posts.Select(x => x.ToApiCompatFeedViewPost()).ToList()
            };
        }

        public enum GetUserPostsFilter
        {
            None,
            posts_with_replies,
            posts_no_replies,
            posts_with_media,
            posts_and_author_threads,
        }
    }
}

