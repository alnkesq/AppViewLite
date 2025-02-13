using AppViewLite.PluggableProtocols;
using AppViewLite.Storage;
using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Linq;

namespace AppViewLite
{
    public class AdministrativeBlocklist
    {

        private FrozenSet<string> BlockDisplay;
        private FrozenSet<string> BlockIngestion;
        private FrozenSet<string> BlockOutboundConnections;

        public AdministrativeBlocklist(IEnumerable<string> preprocessedLines)
        {
            var blockDisplay = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var blockIngestion = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var blockOutboundConnections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            var flags = BlocklistKind.AllowAll;
            var seenFirstSection = false;
            foreach (var line in preprocessedLines)
            {
                if (line[0] == '[')
                {
                    if (!line.EndsWith(']')) throw new ArgumentException("Missing close bracket in blocklist section name.");
                    flags = Enum.Parse<BlocklistKind>(line.AsSpan(1, line.Length - 2), ignoreCase: true);
                    seenFirstSection = true;
                    continue;
                }
                if (!seenFirstSection)
                    throw new ArgumentException("Blocklist file must start with a section name, for example [noinjest,nodisplay,nooutboundconnect]");

                if (line.StartsWith("did:", StringComparison.Ordinal))
                {
                    try
                    {
                        BlueskyEnrichedApis.EnsureValidDid(line);
                    }
                    catch
                    {
                        throw new ArgumentException("Invalid did in blocklist file: " + line);
                    }
                }
                else if (line.Contains('.'))
                {
                    try
                    {
                        BlueskyEnrichedApis.EnsureValidDomain(line);
                    }
                    catch (Exception)
                    {
                        throw new ArgumentException("Invalid domain in blocklist file: " + line);
                    }
                }
                else throw new ArgumentException("Unrecognized line in blocklist file. Only domains and DIDs are supported. Line: " + line);

                if ((flags & BlocklistKind.NoDisplay) != 0) blockDisplay.Add(line);
                if ((flags & BlocklistKind.NoIngest) != 0) blockIngestion.Add(line);
                if ((flags & BlocklistKind.NoOutboundConnect) != 0) blockOutboundConnections.Add(line);

                if (flags == BlocklistKind.AllowAll)
                {
                    blockDisplay.Remove(line);
                    blockIngestion.Remove(line);
                    blockOutboundConnections.Remove(line);
                }
            }
            this.BlockDisplay = blockDisplay.ToFrozenSet(StringComparer.OrdinalIgnoreCase);
            this.BlockIngestion = blockIngestion.ToFrozenSet(StringComparer.OrdinalIgnoreCase);
            this.BlockOutboundConnections = blockOutboundConnections.ToFrozenSet(StringComparer.OrdinalIgnoreCase);
        }


        [Flags]
        public enum BlocklistKind
        {
            AllowAll,

            NoIngest = 1,
            NoDisplay = 2,
            NoOutboundConnect = 4,

            BlockAll = NoIngest | NoDisplay | NoOutboundConnect,
        }

        // You can override this default list by adding an [allowall] section to your blocklist file.
        private readonly static string[] DefaultBlocklist = [

                "[noingest]",

                // ActivityPub -> ActivityPub mirrors 
                "gleasonator.dev", // changes user post URL

                // Bluesky -> ActivityPub mirrors
                "bsky.brid.gy",

                // RSS -> ActivityPub mirrors
                "rss-parrot.net",
                "flipboard.com",

                // Nostr -> ActivityPub mirrors
                "mostr.pub",
                "cash.app",
            ];

        public static ReloadableFile<AdministrativeBlocklist> Instance = new ReloadableFile<AdministrativeBlocklist>(AppViewLiteConfiguration.GetString(AppViewLiteParameter.APPVIEWLITE_BLOCKLIST_PATH), path => 
        {
            return new AdministrativeBlocklist(DefaultBlocklist.Concat(StringUtils.ReadTextFile(path)));
        });

        public bool ShouldBlockDisplay(string? domainOrDid) => ShouldBlock(BlockDisplay, domainOrDid);
        public bool ShouldBlockIngestion(string? domainOrDid) => ShouldBlock(BlockIngestion, domainOrDid);
        public bool ShouldBlockOutboundConnection(string? domainOrDid) => ShouldBlock(BlockOutboundConnections, domainOrDid);

        public void ThrowIfBlockedDisplay(string? domainOrDid) => ThrowIfBlocked(BlockDisplay, domainOrDid);
        public void ThrowIfBlockedIngestion(string? domainOrDid) => ThrowIfBlocked(BlockIngestion, domainOrDid);
        public void ThrowIfBlockedOutboundConnection(string? domainOrDid) => ThrowIfBlocked(BlockOutboundConnections, domainOrDid);

        private static void ThrowIfBlocked(FrozenSet<string> set, string? domainOrDid)
        {
            if (ShouldBlock(set, domainOrDid))
                throw new PermissionException("The specified DID or domain has been blocked by administrative rules.");
        }

        private static bool ShouldBlock(FrozenSet<string> set, string? domainOrDid)
        {
            if (string.IsNullOrEmpty(domainOrDid)) return false;

            if (domainOrDid.StartsWith("did:", StringComparison.OrdinalIgnoreCase))
            {
                if (set.Contains(domainOrDid))
                    return true;

                if (domainOrDid.StartsWith("did:web:", StringComparison.Ordinal) && ShouldBlock(set, domainOrDid.Substring(8)))
                    return true;

                if (PluggableProtocol.TryGetPluggableProtocolForDid(domainOrDid) is { } pluggable)
                {
                    if (pluggable.TryGetDomainForDid(domainOrDid) is { } domain && ShouldBlock(set, domain))
                        return true;
                }

                return false;
            }

            while (true)
            {
                if (set.Contains(domainOrDid)) return true;
                var dot = domainOrDid.IndexOf('.');
                if (dot == -1) break;

                domainOrDid = domainOrDid.Substring(dot + 1);
            }

            return false;
        }
    }
}

