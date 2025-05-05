using AppViewLite.Models;
using FishyFlip.Lexicon;
using FishyFlip.Lexicon.App.Bsky.Actor;
using FishyFlip.Lexicon.App.Bsky.Graph;
using FishyFlip.Lexicon.App.Bsky.Embed;
using FishyFlip.Lexicon.App.Bsky.Feed;
using FishyFlip.Lexicon.App.Bsky.Richtext;
using FishyFlip.Lexicon.Com.Atproto.Repo;
using FishyFlip.Lexicon.Tools.Ozone.Moderation;
using FishyFlip.Models;
using Ipfs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace AppViewLite
{
    public static class ApiCompatUtils
    {
        public static ThreadViewPost ToApiCompatThreadViewPost(this BlueskyPost post, BlueskyPost? rootPost, ThreadViewPost? parent = null)
        {
            return new ThreadViewPost
            {
                Post = post.ToApiCompatPostView(rootPost),
                Parent = parent,
            };
        }
        public static PostView ToApiCompatPostView(this BlueskyPost post, BlueskyPost? rootPost = null)
        {
            var aturi = GetPostUri(post.Author.Did, post.RKey);

            ATObject? embed = null;
            if (post.IsImagePost)
            {
                if (post.Data!.Media![0].IsVideo)
                {
                    //media = new ViewVideo 
                    //{ 
                    //    Alt = post.Data.Media[0].AltText,
                    //};
                }
                else
                {
                    embed = new ViewImages
                    {
                        Images = post.Data.Media.Select(x => ToApiCompatViewImage(x, post.Author)).ToList()
                    };
                }
            }

            if (post.QuotedPost != null)
            {
                var record = ToApiCompatRecordView(post.QuotedPost);
                if (embed != null)
                {
                    embed = new ViewRecordWithMedia { Media = embed, Record = new ViewRecordDef { Record = record }  };
                }
                else
                {
                    embed = record;
                }
            }
            //ATObject? embed = null;

            //if (post.QuotedPost != null)
            //{
            //    embed = new ViewRecord
            //    {

            //    };
            //}
            //if (post.IsImagePost)
            //{
            //    embed = new ViewRecordWithMedia
            //    {
            //         Media
            //    };
            //}

            return new PostView
            {
                Uri = aturi,
                Embed = embed,
                IndexedAt = post.Date,
                RepostCount = post.RepostCount,
                LikeCount = post.LikeCount,
                QuoteCount = post.QuoteCount,
                ReplyCount = post.ReplyCount,
                Cid = GetSyntheticCid(aturi),
                Viewer = new FishyFlip.Lexicon.App.Bsky.Feed.ViewerState
                {
                },
                Record = ToApiCompatPost(post),
                Author = post.Author.ToApiCompatProfileViewBasic(),
            };
        }

        private static RecordView ToApiCompatRecordView(BlueskyPost post)
        {
            var aturi = post.AtUri;
            return new RecordView
            {
                Value = ToApiCompatPost(post),
                Uri = aturi,
                Cid = GetSyntheticCid(aturi),
            };
        }

        private static Post ToApiCompatPost(BlueskyPost post)
        {
            return new Post
            {
                CreatedAt = post.Date,
                Text = post.Data?.Text,
                Reply = post.InReplyToPostId != null ? new ReplyRefDef
                {
                    Parent = GetPostStrongRef(post.InReplyToUser!.Did, post.Data!.InReplyToRKeyString!),
                    Root = GetPostStrongRef(post.RootPostDid!, post.RootPostId.PostRKey.ToString()!)
                } : null,
                Facets = ToApiCompatFacets(post.Data?.Facets, post.Data?.GetUtf8IfNeededByCompactFacets()),
            };
        }

        private static ViewImage ToApiCompatViewImage(BlueskyMediaData x, BlueskyProfile author)
        {
            return new ViewImage
            {
                Alt = x.AltText ?? string.Empty,
                Fullsize = BlueskyEnrichedApis.Instance.GetImageFullUrl(author.Did, x.Cid, author.Pds)!,
                Thumb = BlueskyEnrichedApis.Instance.GetImageThumbnailUrl(author.Did, x.Cid, author.Pds)!,
                AspectRatio = new AspectRatio(1600, 900) // best guess
            };
        }

        private static List<Facet>? ToApiCompatFacets(FacetData[]? facets, byte[]? utf8)
        {
            if (facets == null) return null;
            return facets.Select(x => 
            {
                ATObject? feature = null;
                if (x.Did != null)
                {
                    feature = new Mention { Did = new ATDid(x.Did) };
                }
                else if (x.IsLink)
                {
                    feature = new Link
                    {
                        Uri = x.GetLink(utf8)!
                    };
                }

                if (feature == null) return null;
                return new Facet
                {
                    Index = new ByteSlice(x.Start, x.Length),
                    Features = [feature]
                };
            }).WhereNonNull().ToList();
        }

        public static FeedViewPost ToApiCompatFeedViewPost(this BlueskyPost post)
        {
            return new FeedViewPost
            {
                Post = post.ToApiCompatPostView(null),
                Reply = post.InReplyToFullPost != null ? new ReplyRef
                {
                    Parent = post.InReplyToFullPost.ToApiCompatFeedViewPost(),
                    Root = post.RootFullPost!.ToApiCompatFeedViewPost(),
                    GrandparentAuthor = post.InReplyToFullPost.Author.ToApiCompatProfileViewBasic(),
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

        private readonly static DateTime DummyDate = new DateTime(2023, 2, 1, 0, 0, 0, DateTimeKind.Utc);

        public static ProfileView ToApiCompatProfile(this BlueskyProfile profile)
        {
            return new FishyFlip.Lexicon.App.Bsky.Actor.ProfileView
            {
                DisplayName = profile.DisplayNameOrFallback,
                Labels = [],
                CreatedAt = DummyDate,
                Avatar = GetAvatarUrl(profile),
                Did = new FishyFlip.Models.ATDid(profile.Did),
                Handle = GetHandle(profile),
                Description = profile.BasicData?.Description,
                Viewer = new FishyFlip.Lexicon.App.Bsky.Actor.ViewerState
                {
                    Muted = false,
                    BlockedBy = false,
                },
            };
        }

        private static ATHandle GetHandle(BlueskyProfile profile)
        {
            return new FishyFlip.Models.ATHandle(profile.HandleIsUncertain || profile.PossibleHandle == null ? "handle.invalid" : profile.PossibleHandle);
        }

        private static string? GetAvatarUrl(BlueskyProfile profile)
        {
            return BlueskyEnrichedApis.Instance.GetAvatarUrl(profile.Did, profile.BasicData?.AvatarCidBytes, profile.Pds);
        }
        private static string? GetAvatarUrl(BlueskyFeedGenerator feed)
        {
            return BlueskyEnrichedApis.Instance.GetAvatarUrl(feed.Did, feed.Data?.AvatarCid, feed.Author.Pds);
        }

        public static ProfileViewBasic ToApiCompatProfileViewBasic(this BlueskyProfile profile)
        {
            return new FishyFlip.Lexicon.App.Bsky.Actor.ProfileViewBasic
            {
                DisplayName = profile.DisplayNameOrFallback,
                Labels = [],
                CreatedAt = DummyDate,
                Avatar = GetAvatarUrl(profile),
                Did = new FishyFlip.Models.ATDid(profile.Did),
                Handle = GetHandle(profile),
                Viewer = new FishyFlip.Lexicon.App.Bsky.Actor.ViewerState
                {
                    Muted = false,
                    BlockedBy = false,
                },
            };
        }
        public static ProfileViewDetailed ToApiCompatProfileDetailed(this BlueskyFullProfile fullProfile)
        {
            var profile = fullProfile.Profile;
            return new FishyFlip.Lexicon.App.Bsky.Actor.ProfileViewDetailed
            {
                DisplayName = profile.DisplayNameOrFallback,
                Labels = [],
                CreatedAt = DummyDate,
                Avatar = GetAvatarUrl(profile),
                Did = new FishyFlip.Models.ATDid(profile.Did),
                Handle = GetHandle(profile),
                Description = profile.BasicData?.Description,
                Banner = BlueskyEnrichedApis.Instance.GetImageBannerUrl(profile.Did, profile.BasicData?.BannerCidBytes, profile.Pds),
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

        public static GeneratorView ToApiCompatGeneratorView(this BlueskyFeedGenerator feed, BlueskyProfile creator)
        {
            return new GeneratorView
            {
                Cid = GetSyntheticCid(feed.Uri),
                DisplayName = feed.DisplayName,
                Description = feed.Data?.Description,
                Uri = feed.Uri,
                IndexedAt = DummyDate,
                Did = new ATDid(feed.Did),
                Avatar = GetAvatarUrl(feed),
                AcceptsInteractions = false,
                Creator = creator.ToApiCompatProfile(),

            };
        }

        public static ListView ToApiCompatListView()
        {
            return new ListView
            {
                Cid = GetSyntheticCid(new FishyFlip.Models.ATUri("at://did:web:bad-example.com/com.example.bad/33333")),
                Creator = new ProfileView
                {
                    DisplayName = "[stub]",
                    Labels = [],
                    CreatedAt = DummyDate,
                    Avatar = "https://bad-example.com/stub",
                    Did = new FishyFlip.Models.ATDid("did:plc:hdhoaan3xa3jiuq4fg4mefid"),
                    Handle = new FishyFlip.Models.ATHandle("stub.bad-example.com"),
                    Viewer = new FishyFlip.Lexicon.App.Bsky.Actor.ViewerState
                    {
                        Muted = false,
                        BlockedBy = false,
                    },
                },
                ListItemCount = 0,
                Name = "[stub]",
                Purpose = "app.bsky.graph.defs#curatelist",
                Uri = new FishyFlip.Models.ATUri("at://did:web:bad-example.com/com.example.bad/33333"),
                IndexedAt = DummyDate,
            };
        }

        public static ListItemView ToApiCompatListItemView()
        {
            return new FishyFlip.Lexicon.App.Bsky.Graph.ListItemView
            {
                Subject = new FishyFlip.Lexicon.App.Bsky.Actor.ProfileView
                {
                    DisplayName = "[stub]",
                    Labels = [],
                    CreatedAt = DummyDate,
                    Avatar = "https://bad-example.com/stub",
                    Did = new FishyFlip.Models.ATDid("did:plc:hdhoaan3xa3jiuq4fg4mefid"),
                    Handle = new FishyFlip.Models.ATHandle("stub.bad-example.com"),
                    Viewer = new FishyFlip.Lexicon.App.Bsky.Actor.ViewerState
                    {
                        Muted = false,
                        BlockedBy = false,
                    },
                },
                Uri = new FishyFlip.Models.ATUri("at://did:web:bad-example.com/com.example.bad/33333"),
            };
        }

     
    }
}

