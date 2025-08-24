using System.Collections.Immutable;
using System.Linq;

namespace AppViewLite
{
    public record ParsedMultisessionCookie(string? ActiveDid, ImmutableArray<SessionIdWithUnverifiedDid> Sessions)
    {

        public const string CookieName = "appviewliteSessionId";
        public static ParsedMultisessionCookie Parse(string? cookie)
        {
            if (string.IsNullOrEmpty(cookie)) return new ParsedMultisessionCookie(null, []);
            var parts = cookie.Split('|');

            if (parts.Length == 1)
            {
                var s = SessionIdWithUnverifiedDid.Parse(parts[0]);
                return new ParsedMultisessionCookie(s.UnverifiedDid, [s]);
            }

            var sessions = parts.Skip(1).Select(SessionIdWithUnverifiedDid.Parse).ToImmutableArray();
            return new ParsedMultisessionCookie(parts[0], sessions);
        }

        public override string? ToString()
        {
            if (ActiveDid == null) return null;
            return ActiveDid + "|" + string.Join('|', Sessions);
        }
    }
}



