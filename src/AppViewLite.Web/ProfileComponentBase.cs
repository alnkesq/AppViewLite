using Microsoft.AspNetCore.Components;

namespace AppViewLite.Web
{
    public class ProfileComponentBase : ComponentBase
    {
        [Inject] public required BlueskyEnrichedApis Apis { get; set; }
        [Inject] public required RequestContext RequestContext { get; set; }

        [Parameter] public required string Did { get; set; }
        [Parameter] public string? ActivityPubInstance { get; set; }

        private string? _originalProfileBaseUrl;
        public string ProfileBaseUrl => _originalProfileBaseUrl ?? $"/@{Did}" + (ActivityPubInstance != null ? "@" + ActivityPubInstance : null);

        public async Task ResolveDidAsync()
        {
            _originalProfileBaseUrl = ProfileBaseUrl;
            Did = await Apis.ResolveHandleAsync(Did, ActivityPubInstance, ctx: RequestContext);
            ActivityPubInstance = null;
        }
        public async Task<string> GetResolvedDidAsync()
        {
            return await Apis.ResolveHandleAsync(Did, ActivityPubInstance, ctx: RequestContext);
        }
    }
}

