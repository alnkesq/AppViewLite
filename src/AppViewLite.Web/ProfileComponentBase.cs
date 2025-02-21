using Microsoft.AspNetCore.Components;

namespace AppViewLite.Web
{
    public class ProfileComponentBase : ComponentBase
    {
        [Inject] public required BlueskyEnrichedApis Apis { get; set; }
        [Inject] public required RequestContext RequestContext { get; set; }

        [Parameter] public required string Did { get; set; }
        [Parameter] public string? ActivityPubInstance { get; set; }

        public string ProfileBaseUrl => $"/@{Did}" + (ActivityPubInstance != null ? "@" + ActivityPubInstance : null);

        public async Task ResolveDidAsync()
        {
            Did = await Apis.ResolveHandleAsync(Did, ActivityPubInstance);
            ActivityPubInstance = null;
        }
        public async Task<string> GetResolvedDidAsync()
        {
            return await Apis.ResolveHandleAsync(Did, ActivityPubInstance);
        }
    }
}

