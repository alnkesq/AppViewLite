using ProtoBuf;
using AppViewLite.Numerics;
using System;
using System.Text;
using System.Linq;
using System.Collections.Generic;

namespace AppViewLite.Models
{
    [ProtoContract]
    public class BlueskyPostData
    {
        [ProtoMember(1)] public string? Text;

        [ProtoMember(2)] public int? QuotedPlc;
        [ProtoMember(3)] public long? QuotedRKey;

        [ProtoMember(4)] public int? InReplyToPlc;
        [ProtoMember(5)] public long? InReplyToRKey;

        // [ProtoMember(6)] public string[]? ExternalLinks;
        [ProtoMember(7)] public BlueskyMediaData[]? Media;

        [ProtoIgnore] public string? Error;
        [ProtoIgnore] public bool? Deleted;

        [ProtoMember(8)] public string? ExternalTitle;
        [ProtoMember(9)] public string? ExternalUrl;
        [ProtoMember(10)] public string? ExternalDescription;
        [ProtoMember(11)] public byte[]? ExternalThumbCid;
        [ProtoMember(12)] public string? EmbedRecordUri;

        [ProtoMember(13)] public byte[]? TextBpe;

        [ProtoMember(14)] public FacetData[]? Facets;

        [ProtoMember(15)] public long? RootPostRKey;
        [ProtoMember(16)] public int? RootPostPlc;

        [ProtoMember(17)] public LanguageEnum? Language;

        [ProtoMember(18)] public byte[]? ExternalTitleBpe;
        [ProtoMember(19)] public byte[]? ExternalDescriptionBpe;
        [ProtoMember(20)] public byte[]? ExternalUrlBpe;

        [ProtoMember(21)] public NonQualifiedPluggablePostId? PluggablePostId;
        [ProtoMember(22)] public NonQualifiedPluggablePostId? PluggableInReplyToPostId;
        [ProtoMember(23)] public NonQualifiedPluggablePostId? PluggableRootPostId;
        [ProtoMember(24)] public bool? IsReplyToUnspecifiedPost;
        [ProtoMember(25)] public int? PluggableLikeCount;
        [ProtoMember(26)] public int? PluggableReplyCount;
        [ProtoMember(27)] public int? PluggableLikeCountForScoring;

        // UPDATE IsSlimCandidate if you add fields!


        public bool IsSlimCandidate()
        {
            return
                QuotedRKey == null && // Called when the proto is in a compressed state (no need to check QuotedPlc)
                Media == null &&
                ExternalUrlBpe == null &&
                EmbedRecordUri == null &&
                Facets == null &&
                TextBpe != null &&
                (Language == null || (short)Language <= byte.MaxValue) &&

                PluggablePostId == null &&
                PluggableInReplyToPostId == null &&
                PluggableRootPostId == null &&

                IsReplyToUnspecifiedPost == null &&
                PluggableLikeCount == null &&
                PluggableReplyCount == null &&
                PluggableLikeCountForScoring == null
                ;
        }

        public byte[]? GetUtf8IfNeededByCompactFacets()
        {
            return Facets != null && Facets.Any(x => x.SameLinkAsText == true) ? Encoding.UTF8.GetBytes(Text!) : default;
        }

        internal IEnumerable<string> GetExternalUrls()
        {
            if (ExternalUrl != null) yield return ExternalUrl;

            if (Facets != null)
            {
                foreach (var facet in Facets)
                {
                    if (facet.Link != null) yield return facet.Link;
                    else if (facet.SameLinkAsText == true && Text != null)
                    {
                        yield return facet.GetLink(Encoding.UTF8.GetBytes(Text))!;
                    }
                }
            }
        }

        public string? InReplyToRKeyString => InReplyToRKey != null ? new Tid(InReplyToRKey.Value).ToString() : null;



        [ProtoIgnore] public PostId PostId;

        public PostId? InReplyToPostId => InReplyToRKey != null ? new PostId(new(InReplyToPlc!.Value), new(InReplyToRKey.Value)) : null;
        public PostId RootPostId => RootPostPlc != null ? new PostId(new(RootPostPlc!.Value), new(RootPostRKey!.Value)) : PostId; // best effort for deleted posts

        public PostId? QuotedPostId => QuotedRKey != null ? new PostId(new Plc(QuotedPlc!.Value), new Tid(QuotedRKey.Value)) : null;
    }

    [ProtoContract]
    public class BlueskyPostDataPluggableLikeCountOnly
    {
        [ProtoMember(25)] public int? PluggableLikeCount;
    }
}

