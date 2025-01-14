using FishyFlip.Lexicon.App.Bsky.Actor;
using FishyFlip.Lexicon.App.Bsky.Feed;
using FishyFlip.Lexicon.Com.Atproto.Repo;
using FishyFlip.Models;
using Ipfs;
using AppViewLite.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    public static class ApiCompatUtils
    {
        public static ThreadViewPost ToApiCompatThreadViewPost(this BlueskyPost post, BlueskyPost? rootPost, ThreadViewPost? parent = null)
        {
            return new ThreadViewPost
            {
                 Post = post.ToApiCompat(rootPost),
                 Parent = parent,
            };
        }
        public static PostView ToApiCompat(this BlueskyPost post, BlueskyPost? rootPost)
        {
            var aturi = GetPostUri(post.Author.Did, post.RKey);
            return new PostView
            {
                Uri = aturi,
                IndexedAt = post.Date,
                RepostCount = post.RepostCount,
                LikeCount = post.LikeCount,
                QuoteCount = post.QuoteCount,
                ReplyCount = post.ReplyCount,
                Cid = GetSyntheticCid(aturi),
                Viewer = new FishyFlip.Lexicon.App.Bsky.Feed.ViewerState 
                { 
                },
                Record = new Post
                {
                    CreatedAt = post.Date,
                    Text = post.Data?.Text,
                    Reply = post.IsReply ? new ReplyRefDef 
                    { 
                        Parent = GetPostStrongRef(post.InReplyToUser!.Did, post.Data!.InReplyToRKeyString!),
                        Root = rootPost != null ? GetPostStrongRef(rootPost.Did, rootPost.RKey) : null
                    } : null,
                },
                Author = post.Author.ToApiCompatBasic(),
            };
        }


        public static FeedViewPost ToApiCompatFeedViewPost(this BlueskyPost post) 
        {
            return new FeedViewPost
            {
                 Post = post.ToApiCompat(null),
                 Reply = post.InReplyToFullPost != null ? new ReplyRef
                 {
                       Parent = post.InReplyToFullPost.ToApiCompatFeedViewPost(),
                       Root = post.RootFullPost?.ToApiCompatFeedViewPost(),
                       GrandparentAuthor = post.InReplyToFullPost.Author.ToApiCompatBasic(),
                 } : null
            };
        }
        private static StrongRef GetPostStrongRef(string did, string rkey)
        {
            var uri = GetPostUri(did, rkey);
            return new StrongRef
            {
                Uri = uri,
                Cid = GetSyntheticCid(uri)
            };
        }

        private static string GetSyntheticCid(ATUri uri)
        {
            var c = Cid.Decode("bafyreiaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").ToArray();
            var preservePrefix = 4;
            c.AsSpan(preservePrefix).Clear();
            var hash = SHA256.HashData(Encoding.UTF8.GetBytes(uri.ToString()));
            hash.CopyTo(c.AsSpan(preservePrefix));
            var q = Cid.Read(c);
            return q.ToString();
        }

        private static ATUri GetPostUri(string did, string rkey)
        {
            return new ATUri("at://" + did + "/app.bsky.feed.post/" + rkey);
        }
        public static ProfileView ToApiCompatProfile(this BlueskyProfile profile)
        {
            return new FishyFlip.Lexicon.App.Bsky.Actor.ProfileView
            {
                DisplayName = profile.DisplayNameOrFallback,
                Labels = [],
                CreatedAt = new DateTime(2023, 2, 1, 0, 0, 0, DateTimeKind.Utc),
                Avatar = profile.AvatarUrl,
                Did = new FishyFlip.Models.ATDid(profile.Did),
                Handle = new FishyFlip.Models.ATHandle("handle.invalid"),
                Viewer = new FishyFlip.Lexicon.App.Bsky.Actor.ViewerState
                {
                    Muted = false,
                    BlockedBy = false,
                },
            };
        }
        public static ProfileViewBasic ToApiCompatBasic(this BlueskyProfile profile)
        {
            return new FishyFlip.Lexicon.App.Bsky.Actor.ProfileViewBasic
            {
                DisplayName = profile.DisplayNameOrFallback,
                Labels = [],
                CreatedAt = new DateTime(2023, 2, 1, 0, 0, 0, DateTimeKind.Utc),
                Avatar = profile.AvatarUrl,
                Did = new FishyFlip.Models.ATDid(profile.Did),
                Handle = new FishyFlip.Models.ATHandle("handle.invalid"),
                Viewer = new FishyFlip.Lexicon.App.Bsky.Actor.ViewerState
                {
                    Muted = false,
                    BlockedBy = false,
                },
            };
        }
        public static ProfileViewDetailed ToApiCompatDetailed(this BlueskyFullProfile fullProfile)
        {
            var profile = fullProfile.Profile;
            return new FishyFlip.Lexicon.App.Bsky.Actor.ProfileViewDetailed
            {
                DisplayName = profile.DisplayNameOrFallback,
                Labels = [],
                CreatedAt = new DateTime(2023, 2, 1, 0, 0, 0, DateTimeKind.Utc),
                Avatar = profile.AvatarUrl,
                Did = new FishyFlip.Models.ATDid(profile.Did),
                Handle = new FishyFlip.Models.ATHandle("handle.invalid"),
                Viewer = new FishyFlip.Lexicon.App.Bsky.Actor.ViewerState
                {
                    Muted = false,
                    BlockedBy = false,
                },
                FollowersCount = fullProfile.Followers,
                FollowsCount = fullProfile.Following,
            };
        }

        private static ProfileAssociated CreateProfileAssociated()
        {
            return new FishyFlip.Lexicon.App.Bsky.Actor.ProfileAssociated
            {
                Lists = 0,
                Feedgens = 0,
                StarterPacks = 0,
                Labeler = false,
                Chat = new FishyFlip.Lexicon.App.Bsky.Actor.ProfileAssociatedChat
                {
                    AllowIncoming = "none"
                }
            };
        }
    }
}

