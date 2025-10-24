using ProtoBuf;
using AppViewLite.Storage;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace AppViewLite.Models
{
    [ProtoContract]
    public class AppViewLiteProfileProto
    {
        [ProtoMember(1)] public DateTime? FirstLogin;
        [ProtoMember(2)] public AppViewLiteSessionProto[] Sessions = null!;
        [ProtoMember(3)] public byte[]? PdsSessionCbor;
        [ProtoMember(4)] public PrivateFollow[] PrivateFollows = null!;
        [ProtoMember(5)] public MuteRule[] MuteRules = null!;
        [ProtoMember(6)] public int LastAssignedMuteRuleId;
        [ProtoMember(7)] public Theme Theme;
        [ProtoMember(8)] public AccentColor AccentColor;
        [ProtoMember(9)] public FeedSubscription[] FeedSubscriptions = null!;
        [ProtoMember(10)] public LabelerSubscription[] LabelerSubscriptions = null!;
        [ProtoMember(11)] public bool ImportedFollows;
        [ProtoMember(12)] public bool AlwaysPreferBookmarkButton;

        public ObjectIdentityBasedCache<MuteRule[], ILookup<Plc, MuteRule>>? _muteRulesByPlc;
        public ObjectIdentityBasedCache<MuteRule[], Func<string?, Uri[], MuteRule[]>>? _textCouldContainGlobalMuteWords;
        public ObjectIdentityBasedCache<MuteRule[], HashSet<string>>? _globalPluggableAuthorMuteRules;

        public HashSet<string> GlobalPluggableAuthorMuteRules => ObjectIdentityBasedCache.GetOrCreateCache(MuteRules, ref _globalPluggableAuthorMuteRules, x => x.Where(x => x.AppliesToPlc == null).Select(x => x.PluggableAuthorName).WhereNonNull().ToHashSet(StringComparer.OrdinalIgnoreCase));
        public ILookup<Plc, MuteRule> MuteRulesByPlc => ObjectIdentityBasedCache.GetOrCreateCache(MuteRules, ref _muteRulesByPlc, x => x.Where(x => x.AppliesToPlc != null).ToLookup(x => new Plc(x.AppliesToPlc!.Value)));
        
        public Func<string?, Uri[], MuteRule[]> TextCouldContainGlobalMuteWords => ObjectIdentityBasedCache.GetOrCreateCache(MuteRules, ref _textCouldContainGlobalMuteWords, x =>
        {
            var globalMuteRules = x.Where(x => x.AppliesToPlc == null).ToArray();

            var longestWordForEachGlobalMuteRule =
                globalMuteRules
                .Select(x =>
                {
                    var words = StringUtils.GetDistinctWords(x.Word);
                    if (words.Length == 0) return null;
                    return words.MaxBy(x => x.Length);
                })
                .WhereNonNull()
                .ToArray();
            if (longestWordForEachGlobalMuteRule.Length == 0) return (_, _) => [];

            // SearchValues only supports Ordinal and OrdinalIgnoreCase
            var regex = new Regex(@"\b(?:" + string.Join("|", longestWordForEachGlobalMuteRule.Select(x => Regex.Escape(x))) + @")\b", RegexOptions.NonBacktracking | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
            return (postText, urls) =>
            {
                var isMatch =
                    (postText != null && regex.IsMatch(postText)) ||
                    (urls.Length != 0 && urls.Any(x => regex.IsMatch(x.Host)));
                return isMatch ? globalMuteRules : [];
            };
        });

        internal object GetCountersThreadSafe()
        {
            return new
            {
                FirstLogin,
                Sessions = Sessions.Length,
                PrivateFollows = PrivateFollows.Length,
                MuteRules = MuteRules.Length,
                LastAssignedMuteRuleId,
                FeedSubscriptions = FeedSubscriptions.Length,
                LabelerSubscriptions = LabelerSubscriptions.Length,
            };
        }
    }

    [ProtoContract]
    public class AppViewLiteSessionProto
    {
        [ProtoMember(1)] public required string SessionToken;
        [ProtoMember(2)] public DateTime LastSeen;
        [ProtoMember(3)] public bool IsReadOnlySimulation;
        [ProtoMember(4)] public DateTime LogInDate;

    }

    [ProtoContract]
    public class PrivateFollow
    {
        [ProtoMember(1)] public int Plc;
        [ProtoMember(2)] public PrivateFollowFlags Flags;
        [ProtoMember(3)] public DateTime DatePrivateFollowed;
    }


    [Flags]
    public enum PrivateFollowFlags : ulong
    {
        None = 0,
        PrivateFollow = 1,

        MuteImageSelfReposts = 2,
        MuteTextualSelfReposts = 4,
        MuteAllSelfReposts = MuteImageSelfReposts | MuteTextualSelfReposts,

        MuteImageNonFollowedReposts = 8,
        MuteTextualNonFollowedReposts = 16,
        MuteAllNonFollowedReposts = MuteImageNonFollowedReposts | MuteTextualNonFollowedReposts,

        // 32, 64: never used, can recycle

        MuteImagePosts = 128,
        MuteTextualPosts = 256,
        MuteAllPosts = MuteImagePosts | MuteTextualPosts,
    }


    [ProtoContract]
    public class MuteRule
    {
        [ProtoMember(1)] public required string Word;
        [ProtoMember(2)] public int? AppliesToPlc;
        [ProtoMember(3)] public required int Id;

        public string? PluggableAuthorName => Word.StartsWith("u:", StringComparison.Ordinal) ? Word.Substring(2) : null;

        private Func<string[], Uri[], BlueskyPost, bool>? isMatch;

        public bool AppliesTo(string[] words, Uri[] urls, BlueskyPost post)
        {
            if (this.AppliesToPlc != null)
            {
                var appliesToPlc = new Plc(AppliesToPlc.Value);
                if (!(appliesToPlc == post.AuthorId || appliesToPlc == post.RepostedBy?.Plc))
                    return false;
            }
            isMatch ??= CreateMatcher();
            return isMatch(words, urls, post);
        }

        private Func<string[], Uri[], BlueskyPost, bool> CreateMatcher()
        {
            var pluggableAuthor = PluggableAuthorName;
            if (pluggableAuthor != null)
            {
                return (text, urls, post) =>
                {
                    return string.Equals(post.Data?.PluggableAuthor, pluggableAuthor, StringComparison.OrdinalIgnoreCase);
                };
            }
            if (Word.Contains('.') && !Word.Contains(' '))
            {
                return (text, urls, post) =>
                {
                    return urls.Any(x => x.HasHostSuffix(Word));
                };
            }
            var phrase = StringUtils.GetAllWords(Word).ToArray();
            if (phrase.Length == 0) return (_, _, _) => false;

            if (phrase.Length == 1) return (text, _, _) => text.Contains(phrase[0]);

            return (post, _, _) =>
            {
                var postWords = post.AsSpan();
                while (true)
                {
                    if (postWords.Length == 0) return false;

                    var first = postWords.IndexOf(phrase[0]);
                    if (first == -1) return false;

                    if (postWords.Slice(first + 1).StartsWith(phrase.AsSpan(1)))
                        return true;
                    postWords = postWords.Slice(1);
                }
            };
        }
    }

    public enum Theme
    {
        SystemDefault,
        Light,
        Dark,
    }
    public enum AccentColor
    {
        Blue,
        Red,
        Green,
        Orange,
        Pink,
        Purple,
        Gray,
    }

    [ProtoContract]
    public class FeedSubscription
    {
        [ProtoMember(1)] public int FeedPlc;
        [ProtoMember(2)] public required string FeedRKey;
    }

    [ProtoContract]
    public class LabelerSubscription
    {
        [ProtoMember(1)] public required int LabelerPlc;
        [ProtoMember(2)] public long ListRKey;
        [ProtoMember(3)] public ulong LabelerNameHash;
        [ProtoMember(4)] public required ModerationBehavior Behavior;
        [ProtoMember(5)] public string? OverrideDisplayName;
    }
}

