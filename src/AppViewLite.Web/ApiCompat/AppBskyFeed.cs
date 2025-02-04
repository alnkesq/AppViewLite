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
        private readonly BlueskyEnrichedApis apis;
        public AppBskyFeed(BlueskyEnrichedApis apis)
        {
            this.apis = apis;
        }

        [HttpGet("app.bsky.feed.getPostThread")]
        public async Task<GetPostThreadOutput> GetPostThread(string uri, int depth)
        {

            var aturi = await apis.ResolveUriAsync(uri);
            var thread = (await apis.GetPostThreadAsync(aturi.Did!.Handler, aturi.Rkey, default, null, RequestContext.Create())).Posts;

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
            var aturi = await apis.ResolveUriAsync(uri);
            var likers = await apis.GetPostLikersAsync(aturi.Did!.Handler, aturi.Rkey, cursor, default, RequestContext.Create());
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
            var aturi = await apis.ResolveUriAsync(uri);
            var reposters = await apis.GetPostRepostersAsync(aturi.Did!.Handler, aturi.Rkey, cursor, default, RequestContext.Create());
            return new GetRepostedByOutput
            {
                Uri = aturi,
                Cursor = reposters.NextContinuation,
                RepostedBy = reposters.Profiles.Select(x => x.ToApiCompatProfile()).ToList(),
            };
        }

        [HttpGet("app.bsky.feed.getQuotes")]
        public async Task<GetQuotesOutput> GetQuotes(string uri, string? cursor)
        {
            var aturi = await apis.ResolveUriAsync(uri);
            var quotes = await apis.GetPostQuotesAsync(aturi.Did!.Handler, aturi.Rkey, cursor, default, RequestContext.Create());
            return new GetQuotesOutput
            {
                Uri = aturi,
                Cursor = quotes.NextContinuation,
                Posts = quotes.Posts.Select(x => x.ToApiCompat(null)).ToList(),
            };
        }

        [HttpGet("app.bsky.feed.getFeedGenerator")]
        public async Task<GetFeedGeneratorOutput> GetFeedGenerator(string feed)
        {
            var uri = await apis.ResolveUriAsync(feed);
            var feedDid = uri.Did!.Handler!;
            var feedRKey = uri.Rkey;
            var generator = await apis.GetFeedGeneratorAsync(feedDid, feedRKey);
            var creator = await apis.GetProfileAsync(feedDid, RequestContext.Create());
            return new GetFeedGeneratorOutput
            {
                IsOnline = true,
                IsValid = true,
                View = generator.ToApiCompat(creator)
            };
        }

        [HttpGet("app.bsky.feed.getFeed")]
        public async Task<GetFeedOutput> GetFeed(string feed, int limit, string? cursor)
        {
            var uri = await apis.ResolveUriAsync(feed);
            var feedDid = uri.Did!.Handler!;
            var feedRKey = uri.Rkey;
            var ctx = RequestContext.Create();
            var (posts, displayName, nextContinuation) = await apis.GetFeedAsync(uri.Did!.Handler!, uri.Rkey!, cursor, ctx);
            await apis.PopulateFullInReplyToAsync(posts, ctx);
            return new GetFeedOutput
            {
                Feed = posts.Select(x => x.ToApiCompatFeedViewPost()).ToList(),
                Cursor = nextContinuation,
            };
        }

        [HttpGet("app.bsky.feed.getAuthorFeed")]
        public async Task<GetAuthorFeedOutput> GetAuthorFeed(string actor, GetUserPostsFilter filter, string includePins, int limit, string? cursor)
        {
            if (filter == GetUserPostsFilter.None) filter = GetUserPostsFilter.posts_no_replies;

            if (filter == GetUserPostsFilter.posts_and_author_threads) filter = GetUserPostsFilter.posts_no_replies; // TODO: https://github.com/alnkesq/AppViewLite/issues/16
            var ctx = RequestContext.Create();

            var (posts, nextContinuation) = await apis.GetUserPostsAsync(actor,
                includePosts: true,
                includeReplies: filter != GetUserPostsFilter.posts_no_replies,
                includeReposts: filter == GetUserPostsFilter.posts_no_replies,
                includeLikes: false,
                mediaOnly: filter == GetUserPostsFilter.posts_with_media,
                cursor,
                ctx);
            await apis.PopulateFullInReplyToAsync(posts, ctx);

            return new GetAuthorFeedOutput
            {
                Cursor = nextContinuation,
                Feed = posts.Select(x => x.ToApiCompatFeedViewPost()).ToList()
            };
        }

        [HttpGet("app.bsky.feed.searchPosts")]
        public async Task<SearchPostsOutput> SearchPosts(string q, int limit, SearchPostsSort sort, string? cursor)
        {
            var options = new PostSearchOptions
            {
                Query = q,
            };
            var results = 
                sort == SearchPostsSort.top ? await apis.SearchTopPostsAsync(options, continuation: cursor, limit: limit, ctx: RequestContext.Create()) :
                await apis.SearchLatestPostsAsync(options, continuation: cursor, limit: limit, ctx: RequestContext.Create());
            return new SearchPostsOutput
            {
                 Posts = results.Posts.Select(x => x.ToApiCompat()).ToList(),
                 Cursor = results.NextContinuation,
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

        public enum SearchPostsSort
        { 
            None,
            latest,
            top,
        }
    }
}

