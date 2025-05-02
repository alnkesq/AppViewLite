using FishyFlip.Lexicon;
using FishyFlip.Lexicon.App.Bsky.Feed;
using Microsoft.AspNetCore.Cors;
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
        private readonly BlueskyEnrichedApis apis;
        private readonly RequestContext ctx;
        public AppBskyFeed(BlueskyEnrichedApis apis, RequestContext ctx)
        {
            this.apis = apis;
            this.ctx = ctx;
        }

        [HttpGet("app.bsky.feed.getPostThread")]
        public async Task<IResult> GetPostThread(string uri, int depth)
        {

            var aturi = await apis.ResolveUriAsync(uri, ctx);
            var thread = (await apis.GetPostThreadAsync(aturi.Did!.Handler, aturi.Rkey, default, null, ctx)).Posts;

            var focalPostIndex = thread.ToList().FindIndex(x => x.Did == aturi.Did.Handler && x.RKey == aturi.Rkey);
            if (focalPostIndex == -1) AssertionLiteException.Throw("focalPostIndex is -1");
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
            }.ToJsonResponse();
        }

        [HttpGet("app.bsky.feed.getLikes")]
        public async Task<IResult> GetLikes(string uri, string? cursor)
        {
            var aturi = await apis.ResolveUriAsync(uri, ctx);
            var likers = await apis.GetPostLikersAsync(aturi.Did!.Handler, aturi.Rkey, cursor, default, ctx);
            return new GetLikesOutput
            {
                Uri = aturi,
                Cursor = likers.NextContinuation,
                Likes = likers.Profiles.Select(x => new LikeDef { Actor = x.ToApiCompatProfile(), CreatedAt = x.RelationshipRKey!.Value.Date, IndexedAt = x.RelationshipRKey.Value.Date }).ToList(),
            }.ToJsonResponse();
        }

        [HttpGet("app.bsky.feed.getRepostedBy")]
        public async Task<IResult> GetRepostedBy(string uri, string? cursor)
        {
            var aturi = await apis.ResolveUriAsync(uri, ctx);
            var reposters = await apis.GetPostRepostersAsync(aturi.Did!.Handler, aturi.Rkey, cursor, default, ctx);
            return new GetRepostedByOutput
            {
                Uri = aturi,
                Cursor = reposters.NextContinuation,
                RepostedBy = reposters.Profiles.Select(x => x.ToApiCompatProfile()).ToList(),
            }.ToJsonResponse();
        }

        [HttpGet("app.bsky.feed.getQuotes")]
        public async Task<IResult> GetQuotes(string uri, string? cursor)
        {
            var aturi = await apis.ResolveUriAsync(uri, ctx);
            var quotes = await apis.GetPostQuotesAsync(aturi.Did!.Handler, aturi.Rkey, cursor, default, ctx);
            return new GetQuotesOutput
            {
                Uri = aturi,
                Cursor = quotes.NextContinuation,
                Posts = quotes.Posts.Select(x => x.ToApiCompat(null)).ToList(),
            }.ToJsonResponse();
        }

        [HttpGet("app.bsky.feed.getFeedGenerator")]
        public async Task<IResult> GetFeedGenerator(string feed)
        {
            var uri = await apis.ResolveUriAsync(feed, ctx);
            var feedDid = uri.Did!.Handler!;
            var feedRKey = uri.Rkey;
            var generator = await apis.GetFeedGeneratorAsync(feedDid, feedRKey, ctx);
            var creator = await apis.GetProfileAsync(feedDid, ctx);
            return new GetFeedGeneratorOutput
            {
                IsOnline = true,
                IsValid = true,
                View = generator.ToApiCompat(creator)
            }.ToJsonResponse();
        }

        [HttpGet("app.bsky.feed.getFeed")]
        public async Task<IResult> GetFeed(string feed, int limit, string? cursor)
        {
            var uri = await apis.ResolveUriAsync(feed, ctx);
            var feedDid = uri.Did!.Handler!;
            var feedRKey = uri.Rkey;
            var (posts, info, nextContinuation) = await apis.GetFeedAsync(uri.Did!.Handler!, uri.Rkey!, cursor, ctx);
            await apis.PopulateFullInReplyToAsync(posts, ctx);
            return new GetFeedOutput
            {
                Feed = posts.Select(x => x.ToApiCompatFeedViewPost()).ToList(),
                Cursor = nextContinuation,
            }.ToJsonResponse();
        }

        [HttpGet("app.bsky.feed.getAuthorFeed")]
        public async Task<IResult> GetAuthorFeed(string actor, GetUserPostsFilter filter, string includePins, int limit, string? cursor)
        {
            if (filter == GetUserPostsFilter.None) filter = GetUserPostsFilter.posts_no_replies;

            if (filter == GetUserPostsFilter.posts_and_author_threads) filter = GetUserPostsFilter.posts_no_replies; // TODO: https://github.com/alnkesq/AppViewLite/issues/16

            var (posts, nextContinuation) = await apis.GetUserPostsAsync(actor,
                includePosts: true,
                includeReplies: filter != GetUserPostsFilter.posts_no_replies,
                includeReposts: filter == GetUserPostsFilter.posts_no_replies,
                includeLikes: false,
                includeBookmarks: false,
                mediaOnly: filter == GetUserPostsFilter.posts_with_media,
                limit: limit,
                cursor,
                ctx);
            await apis.PopulateFullInReplyToAsync(posts, ctx);

            return new GetAuthorFeedOutput
            {
                Cursor = nextContinuation,
                Feed = posts.Select(x => x.ToApiCompatFeedViewPost()).ToList()
            }.ToJsonResponse();
        }

        [HttpGet("app.bsky.feed.searchPosts")]
        public async Task<IResult> SearchPosts(string q, int limit, SearchPostsSort sort, string? cursor)
        {
            var options = new PostSearchOptions
            {
                Query = q,
            };
            var results =
                sort == SearchPostsSort.top ? await apis.SearchTopPostsAsync(options, ctx, continuation: cursor, limit: limit) :
                await apis.SearchLatestPostsAsync(options, continuation: cursor, limit: limit, ctx: ctx);
            return new SearchPostsOutput
            {
                Posts = results.Posts.Select(x => x.ToApiCompat()).ToList(),
                Cursor = results.NextContinuation,
            }.ToJsonResponse();
        }

        public enum GetUserPostsFilter
        {
            None,
            posts_with_replies,
            posts_no_replies,
            posts_with_media,
            posts_and_author_threads,
        }

        public enum SearchPostsSort
        {
            None,
            latest,
            top,
        }
    }
}

