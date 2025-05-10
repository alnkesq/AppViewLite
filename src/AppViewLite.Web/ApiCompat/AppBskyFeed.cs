using AppViewLite.Models;
using FishyFlip.Lexicon;
using FishyFlip.Lexicon.App.Bsky.Feed;
using FishyFlip.Models;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using AppViewLite;
using System.Linq;
using System.Threading.Tasks;

namespace AppViewLite.Web.ApiCompat
{
    [ApiController]
    [EnableCors("BskyClient")]
    public class AppBskyFeed : FishyFlip.Xrpc.Lexicon.App.Bsky.Feed.FeedController
    {
        private readonly BlueskyEnrichedApis apis;
        private readonly RequestContext ctx;
        public AppBskyFeed(BlueskyEnrichedApis apis, RequestContext ctx)
        {
            this.apis = apis;
            this.ctx = ctx;
        }

        public override Task<Results<ATResult<DescribeFeedGeneratorOutput>, ATErrorResult>> DescribeFeedGeneratorAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public async override Task<Results<ATResult<GetActorFeedsOutput>, ATErrorResult>> GetActorFeedsAsync([FromQuery] ATIdentifier actor, [FromQuery] int? limit = 50, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            var feeds = await apis.GetProfileFeedsAsync(((ATDid)actor).Handler, cursor, limit ?? default, ctx);

            return new GetActorFeedsOutput
            {
                Feeds = feeds.Feeds.Select(x => ApiCompatUtils.ToApiCompatGeneratorView(x)).ToList(),
                Cursor = feeds.NextContinuation
            }.ToJsonResultOk();
        }

        public async override Task<Results<ATResult<GetActorLikesOutput>, ATErrorResult>> GetActorLikesAsync([FromQuery] ATIdentifier actor, [FromQuery] int? limit = 50, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
                    
            var likes = await apis.GetUserPostsAsync(((ATDid)actor).Handler, includePosts: false, includeReplies: false, includeReposts: false, includeLikes: true, includeBookmarks: false, mediaOnly: false, limit ?? default, cursor, ctx);

            return new GetActorLikesOutput
            {
                Feed = likes.Posts.Select(x => ApiCompatUtils.ToApiCompatFeedViewPost(x, ctx)).ToList(),
                Cursor = likes.NextContinuation
            }.ToJsonResultOk();
        }

        public async override Task<Results<ATResult<GetAuthorFeedOutput>, ATErrorResult>> GetAuthorFeedAsync([FromQuery] ATIdentifier actor, [FromQuery] int? limit = 50, [FromQuery] string? cursor = null, [FromQuery] string? filter = null, [FromQuery] bool? includePins = null, CancellationToken cancellationToken = default)
        {
            var filterEnum = filter != null ? Enum.Parse<GetUserPostsFilter>(filter, ignoreCase: true) : GetUserPostsFilter.posts_and_author_threads;
            if (filterEnum == GetUserPostsFilter.None) filterEnum = GetUserPostsFilter.posts_no_replies;

            if (filterEnum == GetUserPostsFilter.posts_and_author_threads) filterEnum = GetUserPostsFilter.posts_no_replies; // TODO: https://github.com/alnkesq/AppViewLite/issues/16

            var (posts, nextContinuation) = await apis.GetUserPostsAsync(((ATDid)actor).Handler,
                includePosts: true,
                includeReplies: filterEnum != GetUserPostsFilter.posts_no_replies,
                includeReposts: filterEnum == GetUserPostsFilter.posts_no_replies,
                includeLikes: false,
                includeBookmarks: false,
                mediaOnly: filterEnum == GetUserPostsFilter.posts_with_media,
                limit: limit ?? default,
                cursor,
                ctx);
            await apis.PopulateFullInReplyToAsync(posts, ctx);

            return new GetAuthorFeedOutput
            {
                Cursor = nextContinuation,
                Feed = posts.Select(x => x.ToApiCompatFeedViewPost(ctx)).ToList()
            }.ToJsonResultOk();
        }

        public async override Task<Results<ATResult<GetFeedOutput>, ATErrorResult>> GetFeedAsync([FromQuery] ATUri feed, [FromQuery] int? limit = 50, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            var uri = await apis.ResolveUriAsync(feed.ToString(), ctx);
            var feedDid = uri.Did!.Handler!;
            var feedRKey = uri.Rkey;
            var (posts, info, nextContinuation) = await apis.GetFeedAsync(uri.Did!.Handler!, uri.Rkey!, cursor, ctx, limit: limit ?? default);
            await apis.PopulateFullInReplyToAsync(posts, ctx);
            return new GetFeedOutput
            {
                Feed = posts.Select(x => x.ToApiCompatFeedViewPost(ctx)).ToList(),
                Cursor = nextContinuation,
            }.ToJsonResultOk();
        }

        public async override Task<Results<ATResult<GetFeedGeneratorOutput>, ATErrorResult>> GetFeedGeneratorAsync([FromQuery] ATUri feed, CancellationToken cancellationToken = default)
        {
            var uri = await apis.ResolveUriAsync(feed.ToString(), ctx);
            var feedDid = uri.Did!.Handler!;
            var feedRKey = uri.Rkey;
            var generator = await apis.GetFeedGeneratorAsync(feedDid, feedRKey, ctx);
            return new GetFeedGeneratorOutput
            {
                IsOnline = true,
                IsValid = true,
                View = generator.ToApiCompatGeneratorView()
            }.ToJsonResultOk();
        }

        public async override Task<Results<ATResult<GetFeedGeneratorsOutput>, ATErrorResult>> GetFeedGeneratorsAsync([FromQuery] List<ATUri> feeds, CancellationToken cancellationToken = default)
        {
            if (feeds.Count == 0) return new GetFeedGeneratorsOutput { Feeds = [] }.ToJsonResultOk();

            var feedsInfos = apis.WithRelationshipsLockForDids(feeds.Select(x => x.Did!.Handler).ToArray(), (_, rels) =>
            {
                return feeds.Select(x => rels.GetFeedGenerator(rels.SerializeDid(x.Did!.Handler, ctx), x.Rkey, ctx)).ToArray();
            }, ctx);
            await apis.EnrichAsync(feedsInfos, ctx, cancellationToken);

            return new GetFeedGeneratorsOutput
            {
                Feeds = feedsInfos.Select(x => ApiCompatUtils.ToApiCompatGeneratorView(x)).ToList()
            }.ToJsonResultOk();
        }

        public override Task<Results<ATResult<GetFeedSkeletonOutput>, ATErrorResult>> GetFeedSkeletonAsync([FromQuery] ATUri feed, [FromQuery] int? limit = 50, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public async override Task<Results<ATResult<GetLikesOutput>, ATErrorResult>> GetLikesAsync([FromQuery] ATUri uri, [FromQuery] string? cid = null, [FromQuery] int? limit = 50, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            var aturi = await apis.ResolveUriAsync(uri.ToString(), ctx);
            var likers = await apis.GetPostLikersAsync(aturi.Did!.Handler, aturi.Rkey, cursor, default, ctx);
            return new GetLikesOutput
            {
                Uri = aturi,
                Cursor = likers.NextContinuation,
                Likes = likers.Profiles.Select(x => new LikeDef { Actor = x.ToApiCompatProfileView(), CreatedAt = x.RelationshipRKey!.Value.Date, IndexedAt = x.RelationshipRKey.Value.Date }).ToList(),
            }.ToJsonResultOk();
        }

        public override Task<Results<ATResult<GetListFeedOutput>, ATErrorResult>> GetListFeedAsync([FromQuery] ATUri list, [FromQuery] int? limit = 50, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            return new GetListFeedOutput
            {
                Feed = [],
            }.ToJsonResultOkTask();
        }

        public async override Task<Results<ATResult<GetPostsOutput>, ATErrorResult>> GetPostsAsync([FromQuery] List<ATUri> uris, CancellationToken cancellationToken = default)
        {
            var posts = apis.WithRelationshipsLockForDids(uris.Select(x => x.Did!.Handler).ToArray(), (_, rels) =>
            {
                return uris.Select(x => rels.GetPost(x.Did!.Handler, x.Rkey, ctx)).ToArray();

            }, ctx);
            await apis.EnrichAsync(posts, ctx, ct: cancellationToken);

            return new GetPostsOutput
            {
                Posts = posts.Select(x => ApiCompatUtils.ToApiCompatPostView(x, ctx)).ToList(),
            }.ToJsonResultOk();
        }

        public async override Task<Results<ATResult<GetPostThreadOutput>, ATErrorResult>> GetPostThreadAsync([FromQuery] ATUri uri, [FromQuery] int? depth = 6, [FromQuery] int? parentHeight = 80, CancellationToken cancellationToken = default)
        {
            var aturi = await apis.ResolveUriAsync(uri.ToString(), ctx);
            var thread = (await apis.GetPostThreadAsync(aturi.Did!.Handler, aturi.Rkey, default, null, ctx)).Posts;

            var focalPostIndex = thread.ToList().FindIndex(x => x.Did == aturi.Did.Handler && x.RKey == aturi.Rkey);
            if (focalPostIndex == -1) AssertionLiteException.Throw("focalPostIndex is -1");
            var rootPost = thread[0];

            ThreadViewPost? tvp = null;
            for (int i = 0; i <= focalPostIndex; i++)
            {
                tvp = thread[i].ToApiCompatThreadViewPost(ctx, rootPost, tvp);
            }

            tvp!.Replies = thread.Skip(focalPostIndex + 1).Select(x => (ATObject)x.ToApiCompatThreadViewPost(ctx, rootPost)).ToList();
            return new GetPostThreadOutput
            {
                Thread = tvp,
            }.ToJsonResultOk();
        }

        public async override Task<Results<ATResult<GetQuotesOutput>, ATErrorResult>> GetQuotesAsync([FromQuery] ATUri uri, [FromQuery] string? cid = null, [FromQuery] int? limit = 50, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            var aturi = await apis.ResolveUriAsync(uri.ToString(), ctx);
            var quotes = await apis.GetPostQuotesAsync(aturi.Did!.Handler, aturi.Rkey, cursor, limit ?? default, ctx);
            return new GetQuotesOutput
            {
                Uri = aturi,
                Cursor = quotes.NextContinuation,
                Posts = quotes.Posts.Select(x => x.ToApiCompatPostView(ctx, null)).ToList(),
            }.ToJsonResultOk();
        }

        public async override Task<Results<ATResult<GetRepostedByOutput>, ATErrorResult>> GetRepostedByAsync([FromQuery] ATUri uri, [FromQuery] string? cid = null, [FromQuery] int? limit = 50, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            var aturi = await apis.ResolveUriAsync(uri.ToString(), ctx);
            var reposters = await apis.GetPostRepostersAsync(aturi.Did!.Handler, aturi.Rkey, cursor, limit ?? default, ctx);
            return new GetRepostedByOutput
            {
                Uri = aturi,
                Cursor = reposters.NextContinuation,
                RepostedBy = reposters.Profiles.Select(x => x.ToApiCompatProfileView()).ToList(),
            }.ToJsonResultOk();
        }

        public override Task<Results<ATResult<GetSuggestedFeedsOutput>, ATErrorResult>> GetSuggestedFeedsAsync([FromQuery] int? limit = 50, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public async override Task<Results<ATResult<GetTimelineOutput>, ATErrorResult>> GetTimelineAsync([FromQuery] string? algorithm = null, [FromQuery] int? limit = 50, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            var feed = await apis.GetFollowingFeedAsync(cursor, limit ?? default, atProtoOnlyPosts: true, ctx);
            return new GetTimelineOutput
            {
                Cursor = feed.NextContinuation,
                Feed = feed.Posts.Select(x => ApiCompatUtils.ToApiCompatFeedViewPost(x, ctx)).ToList()
            }.ToJsonResultOk();
        }

        public async override Task<Results<ATResult<SearchPostsOutput>, ATErrorResult>> SearchPostsAsync([FromQuery] string q, [FromQuery] string? sort = null, [FromQuery] string? since = null, [FromQuery] string? until = null, [FromQuery] ATIdentifier? mentions = null, [FromQuery] ATIdentifier? author = null, [FromQuery] string? lang = null, [FromQuery] string? domain = null, [FromQuery] string? url = null, [FromQuery] List<string>? tag = null, [FromQuery] int? limit = 25, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            var options = new PostSearchOptions
            {
                Query = q,
            };
            var results =
                sort == "top" ? await apis.SearchTopPostsAsync(options, ctx, continuation: cursor, limit: limit ?? 0) :
                await apis.SearchLatestPostsAsync(options, continuation: cursor, limit: limit ?? 0, ctx: ctx);
            return new SearchPostsOutput
            {
                Posts = results.Posts.Select(x => x.ToApiCompatPostView(ctx)).ToList(),
                Cursor = results.NextContinuation,
            }.ToJsonResultOk();
        }

        public override Task<Results<ATResult<SendInteractionsOutput>, ATErrorResult>> SendInteractionsAsync([FromBody] SendInteractionsInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
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

