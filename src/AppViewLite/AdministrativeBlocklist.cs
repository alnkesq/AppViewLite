using AppViewLite.Models;
using AppViewLite.PluggableProtocols;
using AppViewLite.Storage;
using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Text.RegularExpressions;

namespace AppViewLite
{
    public class AdministrativeBlocklist
    {

        private FrozenSetAndRegexList BlockDisplay;
        private FrozenSetAndRegexList BlockIngestion;
        private FrozenSetAndRegexList BlockOutboundConnections;

        public AdministrativeBlocklist(IEnumerable<string> preprocessedLines)
        {
            var blockDisplay = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var blockIngestion = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var blockOutboundConnections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            var blockDisplayRegex = new List<Regex>();
            var blockIngestionRegex = new List<Regex>();
            var blockOutboundConnectionsRegex = new List<Regex>();


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
                    throw new ArgumentException("Blocklist file must start with a section name, for example [noingest,nodisplay,nooutboundconnect]");


                if (line.StartsWith("regex:", StringComparison.Ordinal))
                {
                    Regex regex;
                    try
                    {
                        regex = new Regex(line.Substring(6).Trim(), RegexOptions.IgnoreCase);
                    }
                    catch (Exception ex)
                    {
                        throw new ArgumentException("Regex rule in blocklist file could not be parsed: " + line + ": " + ex.Message);
                    }

                    if ((flags & BlocklistKind.NoDisplay) != 0) blockDisplayRegex.Add(regex);
                    if ((flags & BlocklistKind.NoIngest) != 0) blockIngestionRegex.Add(regex);
                    if ((flags & BlocklistKind.NoOutboundConnect) != 0) blockOutboundConnectionsRegex.Add(regex);

                    continue;
                }
                
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
                else throw new ArgumentException(@"Unrecognized line in blocklist file. Only domains, DIDs, and ""regex:""-prefixed regexes are supported. Line: " + line);

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
            this.BlockDisplay = new(blockDisplay.ToFrozenSet(StringComparer.OrdinalIgnoreCase), blockDisplayRegex.ToArray());
            this.BlockIngestion = new(blockIngestion.ToFrozenSet(StringComparer.OrdinalIgnoreCase), blockIngestionRegex.ToArray());
            this.BlockOutboundConnections = new(blockOutboundConnections.ToFrozenSet(StringComparer.OrdinalIgnoreCase), blockOutboundConnectionsRegex.ToArray());
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
                "rss-mstdn.studiofreesia.com",

                // Nostr -> ActivityPub mirrors
                "mostr.pub",
                "cash.app",
                "nostrplebs.com",
            ];

        public static ReloadableFile<AdministrativeBlocklist> Instance = new ReloadableFile<AdministrativeBlocklist>(AppViewLiteConfiguration.GetString(AppViewLiteParameter.APPVIEWLITE_BLOCKLIST_PATH), path => 
        {
            return new AdministrativeBlocklist(DefaultBlocklist.Concat(StringUtils.ReadTextFile(path)));
        });




        [Pure] public bool ShouldBlockDisplay(string? domainOrDid, DidDocProto? didDoc = null) => ShouldBlock(BlockDisplay, domainOrDid, didDoc);
        [Pure] public bool ShouldBlockIngestion(string? domainOrDid, DidDocProto? didDoc = null) => ShouldBlock(BlockIngestion, domainOrDid, didDoc);
        [Pure] public bool ShouldBlockOutboundConnection(string? domainOrDid, DidDocProto? didDoc = null) => ShouldBlock(BlockOutboundConnections, domainOrDid, didDoc);

        [Pure]
        private static bool ShouldBlock(FrozenSetAndRegexList set, string? domainOrDid, DidDocProto? didDoc)
        {
            if (ShouldBlock(set, domainOrDid)) return true;
            if (didDoc != null)
            {
                foreach (var domain in didDoc.AllHandlesAndDomains)
                {
                    if (ShouldBlock(set, domain)) return true;
                }
            }

            return false;
        }

        public void ThrowIfBlockedDisplay(string? domainOrDid, DidDocProto? didDoc = null) => ThrowIfBlocked(BlockDisplay, domainOrDid);
        public void ThrowIfBlockedIngestion(string? domainOrDid, DidDocProto? didDoc = null) => ThrowIfBlocked(BlockIngestion, domainOrDid);
        public void ThrowIfBlockedOutboundConnection(string? domainOrDid, DidDocProto? didDoc = null) => ThrowIfBlocked(BlockOutboundConnections, domainOrDid);

        private static void ThrowIfBlocked(FrozenSetAndRegexList set, string? domainOrDid, DidDocProto? didDoc = null)
        {
            if (ShouldBlock(set, domainOrDid, didDoc))
                throw new PermissionException("The specified DID or domain has been blocked by administrative rules.");
        }

        private static bool ShouldBlock(FrozenSetAndRegexList rules, string? domainOrDid)
        {
            if (string.IsNullOrEmpty(domainOrDid)) return false;

            if (domainOrDid.StartsWith("did:", StringComparison.OrdinalIgnoreCase))
            {
                if (rules.IsMatch(domainOrDid))
                    return true;

                if (domainOrDid.StartsWith("did:web:", StringComparison.Ordinal) && ShouldBlock(rules, domainOrDid.Substring(8)))
                    return true;

                if (PluggableProtocol.TryGetPluggableProtocolForDid(domainOrDid) is { } pluggable)
                {
                    if (pluggable.TryGetDomainForDid(domainOrDid) is { } domain && ShouldBlock(rules, domain))
                        return true;
                }

                return false;
            }

            while (true)
            {
                if (rules.IsMatch(domainOrDid)) return true;
                var dot = domainOrDid.IndexOf('.');
                if (dot == -1) break;

                domainOrDid = domainOrDid.Substring(dot + 1);
            }

            return false;
        }

        readonly record struct FrozenSetAndRegexList(FrozenSet<string> Set, Regex[] Regexes)
        {
            public bool IsMatch(string domainOrDid)
            {
                return Set.Contains(domainOrDid) || Regexes.Any(x => x.IsMatch(domainOrDid));
            }
        }
    }
}

