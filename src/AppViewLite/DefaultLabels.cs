using AppViewLite.Models;
using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    public static class DefaultLabels
    {
        // Display names and descriptions from
        // https://github.com/bluesky-social/social-app/blob/7d6adb416af9db37aef7d14a9537cab9a3e83602/src/lib/moderation/useGlobalLabelStrings.ts
        public readonly static FrozenDictionary<ulong, BlueskyLabelData> DefaultLabelData = new Dictionary<string, BlueskyLabelData>()
        {
            { "!hide", new BlueskyLabelData("Content Blocked", "This content has been hidden by the moderators.", BlueskyLabelSeverity.Alert) },
            { "!warn", new BlueskyLabelData("Content Warning", "This content has received a general warning from moderators.", BlueskyLabelSeverity.Alert) },
            { "!no-unauthenticated", new BlueskyLabelData("Sign-in Required", "This user has requested that their content only be shown to signed-in users.", BlueskyLabelSeverity.Inform) },
            { "!takedown", new BlueskyLabelData("Taken Down", "This content was taken down by the moderators", BlueskyLabelSeverity.Alert) },

            { "porn", new BlueskyLabelData("Adult Content", "Explicit sexual images.") },
            { "sexual", new BlueskyLabelData("Sexually Suggestive", "Does not include nudity.") },
            { "nudity", new BlueskyLabelData("Non-sexual Nudity", "E.g. artistic nudes.") },
            { "graphic-media", new BlueskyLabelData("Graphic Media", "Explicit or potentially disturbing media.", BlueskyLabelSeverity.Alert) },
        }.ToFrozenDictionary(x => BlueskyRelationships.HashLabelName(x.Key), x => x.Value);

        public static string? GetErrorForAccountState(AccountState accountState, string? pds)
        {
            string? pdsSuffix = null;
            if (pds != null)
            {
                var pdsUri = new Uri(pds);
                if (pdsUri.HasHostSuffix("host.bsky.network"))
                {
                    pdsSuffix = " (Bluesky PBC)";
                }
                else
                {
                    pdsSuffix = " (" + StringUtils.GetDisplayHost(pdsUri) + ")";
                }
            }
            return accountState switch
            {
                AccountState.Unknown => null,
                AccountState.Active => null,
                AccountState.NotActive => $"This account is deactivated/deleted, or was taken down by their PDS{pdsSuffix}.",
                AccountState.TakenDown => $"This account was taken down by their PDS{pdsSuffix}.",
                AccountState.Suspended => $"This account was suspended by their PDS{pdsSuffix}.",
                AccountState.Deleted => "This user deleted their account.",
                AccountState.Deactivated => "This user deactivated their account.",
                AccountState.Desynchronized => "This account is desynchronized.", // what does it mean?
                AccountState.Throttled => "This account is throttled.",
                AccountState.DisabledByAppViewLiteAdministrativeRules => "This account is not displayed because of administrative rules on the current AppViewLite instance.",
                _ => accountState.ToString(),
            };
        }
    }
}

