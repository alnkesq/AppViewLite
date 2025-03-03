using ProtoBuf;
using AppViewLite.Storage;
using System;
using System.Collections.Generic;
using System.Linq;

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
        [ProtoMember(3)] public int Id;

        private Func<string[], Uri[], BlueskyPost, bool>? isMatch;
        
        public bool AppliesTo(string[] words, Uri[] urls, BlueskyPost post)
        {
            if (this.AppliesToPlc != null && new Plc(AppliesToPlc.Value) != post.AuthorId) return false;
            isMatch ??= CreateMatcher();
            return isMatch(words, urls, post);
        }

        private Func<string[], Uri[], BlueskyPost, bool> CreateMatcher()
        {

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
}

