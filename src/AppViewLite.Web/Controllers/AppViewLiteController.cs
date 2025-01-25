using FishyFlip.Lexicon.App.Bsky.Actor;
using FishyFlip.Lexicon.App.Bsky.Feed;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using AppViewLite.Numerics;

namespace AppViewLite.Web
{
    [Route("/api")]
    [ApiController]
    public class AppViewLiteController : ControllerBase
    {
        private readonly RequestContext ctx;

        public AppViewLiteController(RequestContext requestContext)
        {
            this.ctx = requestContext;
        }

        [HttpPost(nameof(CreatePostLike))]
        public async Task<object> CreatePostLike([FromBody] DidAndRKey postId)
        {
            return new
            {
                rkey = (await BlueskyEnrichedApis.Instance.CreatePostLikeAsync(postId.Did, Tid.Parse(postId.Rkey), ctx)).ToString()
            };
        }
        [HttpPost(nameof(DeletePostLike))]
        public async Task DeletePostLike([FromBody] RKeyOnly rkey)
        {
            await BlueskyEnrichedApis.Instance.DeletePostLikeAsync(Tid.Parse(rkey.Rkey), ctx);
        }

        [HttpPost(nameof(CreateRepost))]
        public async Task<object> CreateRepost([FromBody] DidAndRKey postId)
        {
            return new
            {
                rkey = (await BlueskyEnrichedApis.Instance.CreateRepostAsync(postId.Did, Tid.Parse(postId.Rkey), ctx)).ToString()
            };
        }
        [HttpPost(nameof(DeleteRepost))]
        public async Task DeleteRepost([FromBody] RKeyOnly rkey)
        {
            await BlueskyEnrichedApis.Instance.DeleteRepostAsync(Tid.Parse(rkey.Rkey), ctx);
        }
        [HttpPost(nameof(DeletePost))]
        public async Task DeletePost([FromBody] RKeyOnly rkey)
        {
            await BlueskyEnrichedApis.Instance.DeletePostAsync(Tid.Parse(rkey.Rkey), ctx);
        }
    }

    public record DidAndRKey(string Did, string Rkey);
    public record RKeyOnly(string Rkey);
}

