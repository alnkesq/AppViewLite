using AppViewLite.Models;
using AppViewLite.Storage;
using DuckDbSharp;
using DuckDbSharp.Types;
using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Linq;

namespace AppViewLite
{
    internal class Badges
    {
        public const string KindGovernment = "verified-government";
        public const string KindOrganization = "verified-organization";
        public const string KindGeneric = "verified-generic";

        public static ReloadableFile<FrozenDictionary<string, ProfileBadge>> BadgeOverrides = new(AppViewLiteConfiguration.GetString(AppViewLiteParameter.APPVIEWLITE_BADGE_OVERRIDE_PATH), x => StringUtils.ReadTextFile(x).Select(x =>
        {
            var fields = x.Split(",", StringSplitOptions.TrimEntries);
            if (fields.Length < 2) throw new Exception("Invalid badge override data: insufficient number of fields for " + x);
            var handleOrDid = fields[0];
            var kind = fields[1];
            var url = fields.ElementAtOrDefault(2);
            var description = fields.ElementAtOrDefault(3);
            if (string.IsNullOrEmpty(handleOrDid) || string.IsNullOrEmpty(kind))
                throw new Exception("Invalid badge override data: missing DID/handle or badge kind.");
            var isHandleBased = !handleOrDid.StartsWith("did:", StringComparison.Ordinal);
            var badge = new ProfileBadge {
                Kind = kind,
                IsHandleBased = isHandleBased,
                Handle = isHandleBased ? handleOrDid : null,
                Did = isHandleBased ? null : handleOrDid,
                Url = !string.IsNullOrEmpty(url) ? url : null,
                Description = !string.IsNullOrEmpty(description) ? description : null,
            };
            return badge;
        }).ToFrozenDictionary(x => (x.Handle ?? x.Did)!));

        private static string? WikidataPath = AppViewLiteConfiguration.GetString(AppViewLiteParameter.APPVIEWLITE_WIKIDATA_VERIFICATION);

        public static ReloadableFile<ILookup<DuckDbUuid, ProfileBadgeWikidataParquet>>? Wikidata =
            WikidataPath != null ? 
            new ReloadableFile<ILookup<DuckDbUuid, ProfileBadgeWikidataParquet>>(WikidataPath, path => 
            {
                return DuckDbUtils.QueryParquet<ProfileBadgeWikidataParquet>(path!)
                    .ToLookup(x => StringUtils.HashUnicodeToUuid(StringUtils.NormalizeHandle(x.Handle!)), x => 
                    {
                        x.Handle = null;
                        return x;
                    });
            }) : null;

        internal static ProfileBadge[] GetBadges(Plc plc, string did, string? possibleHandle)
        {
            var result = new List<ProfileBadge>();
            possibleHandle = possibleHandle != null ? StringUtils.NormalizeHandle(possibleHandle) : null;

            if (BadgeOverrides.GetValue().TryGetValue(did, out var badgeOverride) || (possibleHandle != null && BadgeOverrides.GetValue().TryGetValue(possibleHandle, out badgeOverride)))
            {
                if (badgeOverride.Kind == "none")
                    return [];
                return [badgeOverride];
            }

            ProfileBadge? badge = null;
            if (possibleHandle != null && IsGovDomain(possibleHandle, out var tld))
            {
                badge = new ProfileBadge
                {
                    IsHandleBased = true,
                    Kind = KindGovernment,
                    Description = $"Government account or elected official ({tld})",
                    Url = possibleHandle.EndsWith(".gov", StringComparison.Ordinal) ? "https://en.wikipedia.org/wiki/.gov" : "https://en.wikipedia.org/wiki/.gov#International_equivalents"
                };
            }

            if (badge == null && possibleHandle != null && Wikidata != null)
            {
                var rows = Wikidata.GetValue()[StringUtils.HashUnicodeToUuid(possibleHandle)].ToArray();
                var wikidataId = rows.Min(x => (long?)x.WikidataId);
                if (wikidataId != null)
                {
                    rows = rows.Where(x => x.WikidataId == wikidataId).ToArray();
                    var isGov = rows.Any(x => x.IsGov);
                    var isOrg = rows.Any(x => x.IsOrganization);
                    var typeString = possibleHandle.EndsWith(".bsky.social", StringComparison.Ordinal) ? "account" : "domain";
                    badge = new ProfileBadge
                    {
                        Kind = isGov ? KindGovernment : isOrg ? KindOrganization : KindGeneric,
                        Description = 
                            isGov ? $"Official government {typeString} (according to Wikidata)" :
                            isOrg ? $"Official organization {typeString} (according to Wikidata)" :
                            $"Official {typeString} (according to Wikidata)",
                        Url = "https://www.wikidata.org/wiki/Q" + wikidataId,
                        IsHandleBased = true,
                    };
                }
            }

            if (badge != null)
            {
                result.Add(badge);
            }

            return result.ToArray();
        }

        private static bool IsGovDomain(string handle, out string? tld)
        {
            while (true)
            {
                if (GovTlds.Contains(handle))
                {
                    tld = "." + handle;
                    return true;
                }
                var dot = handle.IndexOf('.');
                if (dot == -1)
                {
                    tld = null;
                    return false;
                }
                handle = handle.Substring(dot + 1);
            }
            
        }

        // https://en.wikipedia.org/wiki/.gov#International_equivalents
        // Excludes gov.{xx}.ca (varies by region code)

        private readonly static FrozenSet<string> GovTlds = [
            "gov", // manual addition


            "gov.af",
            "gov.al",
            "gov.dz",
            "gov.ad",
            "gov.ao",
            "gov.ai",
            "gov.am",
            "gov.aw",
            "gob.ar",
            "gv.at",
            "gov.au",
            "gov.ax",
            "gov.az",
            "gov.bs",
            "gov.bd",
            "gov.bb",
            "gov.by",
            "gov.be",
            "gov.bg",
            "gov.ba",
            "gov.br",
            "gob.cl",
            "gov.cl",
            "gc.ca",
            "gnb.ca",
            "gouv.qc.ca",
            "gov.cn",
            "gov.hk",
            "gov.mo",
            "gov.co",
            "gov.hr",
            "gov.cy",
            "gov.cz",
            "gov.eg",
            "gob.sv",
            "gov.gr",
            "gov.fi",
            "gouv.fr",
            "gov.hu",
            "gov.in",
            "go.id",
            "gov.ir",
            "gov.iq",
            "gov.krd",
            "gov.ie",
            "gov.il",
            "gov.it",
            "go.jp",
            "gov.kz",
            "go.ke",
            "gov.lv",
            "gov.lb",
            "gov.lt",
            "gov.my",
            "gov.mt",
            "gob.mx",
            "gov.md",
            "gov.ma",
            "gov.mm",
            "gov.np",
            "gouv.nc",
            "govt.nz",
            "gov.ng",
            "gov.kp",
            "gov.ps",
            "gov.py",
            "gob.pe",
            "gov.pk",
            "gov.ph",
            "gov.pl",
            "gov.pt",
            "gov.ro",
            "gov.ru",
            "gouv.sn",
            "gov.sg",
            "gov.sk",
            "gov.si",
            "gov.za",
            "go.kr",
            "gob.es",
            "gov.lk",
            "gov.se",
            "admin.ch",
            "gov.tw",
            "go.th",
            "gov.to",
            "gov.tt",
            "gov.tr",
            "gov.ua",
            "gov.uk",
            "gov.scot",
            "gov.wales",
            "gov.gg",
            "gov.je",
            "gov.im",
            "gov.bm",
            "gov.vg",
            "gov.ky",
            "gov.fk",
            "government.pn",
            "gov.tc",
            "gub.uy",
            "gob.ve",
            "gov.vn",
            ];
    }
}

