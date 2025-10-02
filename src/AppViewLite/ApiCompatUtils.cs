using AppViewLite.Models;
using FishyFlip;
using FishyFlip.Lexicon;
using FishyFlip.Lexicon.App.Bsky.Actor;
using FishyFlip.Lexicon.App.Bsky.Embed;
using FishyFlip.Lexicon.App.Bsky.Feed;
using FishyFlip.Lexicon.App.Bsky.Graph;
using FishyFlip.Lexicon.App.Bsky.Richtext;
using FishyFlip.Lexicon.Com.Atproto.Repo;
using FishyFlip.Models;
using Ipfs;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    public static class ApiCompatUtils
    {
        public static ThreadViewPost ToApiCompatThreadViewPost(this BlueskyPost post, RequestContext ctx, BlueskyPost? rootPost, ThreadViewPost? parent = null)
        {
            return new ThreadViewPost
            {
                Post = post.ToApiCompatPostView(ctx, rootPost),
                Parent = parent,
            };
        }
        public static PostView ToApiCompatPostView(this BlueskyPost post, RequestContext ctx, BlueskyPost? rootPost = null)
        {
            var aturi = GetPostUri(post.Author.Did, post.RKey);

            return new PostView
            {
                Uri = aturi,
                Embed = ToApiCompatEmbedView(post),
                IndexedAt = post.Date,
                RepostCount = post.RepostCount,
                LikeCount = post.LikeCount,
                QuoteCount = post.QuoteCount,
                ReplyCount = post.ReplyCount,
                Cid = GetSyntheticCid(aturi),
                Viewer = ToApiCompatPostViewerState(post, ctx),
                Record = ToApiCompatPost(post),
                Author = post.Author.ToApiCompatProfileViewBasic(),
            };
        }

        private static ATObject? ToApiCompatEmbedView(BlueskyPost post)
        {

            /*
             Must return one of:
                ViewImages
                ViewVideo
                ViewExternal
                ViewRecordDef
                ViewRecordWithMedia
             */
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

            if (post.Data?.ExternalUrl != null)
            {
                embed = new ViewExternal
                {
                    External = new ViewExternalExternal
                    {
                        Description = post.Data.ExternalDescription ?? string.Empty,
                        Title = post.Data.ExternalTitle ?? string.Empty,
                        Uri = post.Data.ExternalUrl,
                        Thumb = BlueskyEnrichedApis.Instance.GetExternalThumbnailUrl(post),
                    }
                };
            }
            if (post.QuotedPost != null)
            {
                var recordView = new ViewRecordDef { Record = ToApiCompatRecordView(post.QuotedPost) };
                if (embed != null)
                {
                    embed = new ViewRecordWithMedia { Media = embed, Record = recordView };
                }
                else
                {
                    embed = recordView;
                }
            }
            return embed;
        }

        private static ATObject? ToApiCompatEmbed(BlueskyPost post)
        {
            /*
             Must return one of:
                EmbedImages
                EmbedVideo
                EmbedExternal
                EmbedRecord
                RecordWithMedia
             */
            ATObject? embed = null;
            if (post.IsImagePost)
            {
                if (post.Data!.Media![0].IsVideo)
                {
                    //media = new EmbedVideo 
                    //{ 
                    //    Alt = post.Data.Media[0].AltText,
                    //};
                }
                else
                {
                    embed = new EmbedImages
                    {
                        Images = post.Data.Media.Select(x => ToApiCompatImage(x)).ToList()
                    };
                }
            }

            if (post.Data?.ExternalUrl != null)
            {
                embed = new EmbedExternal
                {
                    External = new External
                    {
                        Description = post.Data.ExternalDescription ?? string.Empty,
                        Title = post.Data.ExternalTitle ?? string.Empty,
                        Uri = post.Data.ExternalUrl,
                        Thumb = post.Data.ExternalThumbCid != null ? new Blob
                        {
                            Ref = new ATLinkRef(ATCid.Read(post.Data.ExternalThumbCid)),
                            MimeType = "image/jpeg"
                        } : null
                    }
                };
            }

            if (post.QuotedPost != null)
            {
                var record = new EmbedRecord { Record = GetPostStrongRef(post.QuotedPost!.Did, post.QuotedPost.RKey) };
                if (embed != null)
                {
                    embed = new RecordWithMedia { Media = embed, Record = record };
                }
                else
                {
                    embed = record;
                }
            }
            return embed;
        }


        private static FishyFlip.Lexicon.App.Bsky.Feed.ViewerState ToApiCompatPostViewerState(BlueskyPost post, RequestContext ctx)
        {
            return new FishyFlip.Lexicon.App.Bsky.Feed.ViewerState
            {
                Like = post.IsLikedBySelf != null ? new ATUri("at://" + ctx.UserContext.Did + "/" + Like.RecordType + "/" + post.IsLikedBySelf.Value) : null,
                Repost = post.IsRepostedBySelf != null ? new ATUri("at://" + ctx.UserContext.Did + "/" + Repost.RecordType + "/" + post.IsRepostedBySelf.Value) : null,
            };
        }

        private static ViewRecord ToApiCompatRecordView(BlueskyPost post)
        {
            var aturi = post.AtUri;
            return new ViewRecord
            {
                Value = ToApiCompatPost(post),
                Uri = aturi,
                Cid = GetSyntheticCid(aturi),
                Author = ToApiCompatProfileViewBasic(post.Author),
                IndexedAt = DateTime.UtcNow,
                RepostCount = post.RepostCount,
                LikeCount = post.LikeCount,
                QuoteCount = post.QuoteCount,
                ReplyCount = post.ReplyCount,
                Labels = [],
                Embeds = ToApiCompatEmbedView(post) is { } v ? [v] : [],
            };
        }

        public static Post ToApiCompatPost(BlueskyPost post)
        {
            return new Post
            {
                CreatedAt = post.Date,
                Text = post.Data != null ? (post.Data.Error != null ? "[" + post.Data.Error + "]" : post.Data.Text) : "[Post data not loaded yet, please refresh page or post]",
                Reply = post.InReplyToPostId != null ? new ReplyRefDef
                {
                    Parent = GetPostStrongRef(post.InReplyToUser!.Did, post.Data!.InReplyToRKeyString!),
                    Root = GetPostStrongRef(post.RootPostDid!, post.RootPostId.PostRKey.ToString()!)
                } : null,
                Facets = ToApiCompatFacets(post.Data?.Facets, post.Data?.GetUtf8IfNeededByCompactFacets()),
                Langs = post.Data?.Language != null ? [ToApiCompatLanguage(post.Data.Language.Value)] : null,
                Embed = ToApiCompatEmbed(post)
            };
        }

        private static string ToApiCompatLanguage(LanguageEnum language)
        {
            return language.ToString().Replace("_", "-");
        }


        private static Image ToApiCompatImage(BlueskyMediaData x)
        {
            return new Image
            {
                Alt = x.AltText ?? string.Empty,
                ImageValue = new Blob
                {
                    Ref = new ATLinkRef(ATCid.Read(x.Cid)),
                },
                AspectRatio = GetDefaultAspectRatio()
            };
        }

        private static AspectRatio GetDefaultAspectRatio()
        {
            return new AspectRatio(1600, 900); // best guess
        }

        private static ViewImage ToApiCompatViewImage(BlueskyMediaData x, BlueskyProfile author)
        {
            return new ViewImage
            {
                Alt = x.AltText ?? string.Empty,
                Fullsize = BlueskyEnrichedApis.Instance.GetImageFullUrl(author.Did, x.Cid, author.Pds)!,
                Thumb = BlueskyEnrichedApis.Instance.GetImageThumbnailUrl(author.Did, x.Cid, author.Pds)!,
                AspectRatio = GetDefaultAspectRatio(),
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
                    Index = new ByteSlice(x.Start, x.Start + x.Length),
                    Features = [feature]
                };
            }).WhereNonNull().ToList();
        }

        public static FeedViewPost ToApiCompatFeedViewPost(this BlueskyPost post, RequestContext ctx)
        {
            return new FeedViewPost
            {
                Post = post.ToApiCompatPostView(ctx, null),
                Reply = post.InReplyToFullPost != null ? new ReplyRef
                {
                    Parent = post.InReplyToFullPost.ToApiCompatFeedViewPost(ctx),
                    Root = post.RootFullPost!.ToApiCompatFeedViewPost(ctx),
                    GrandparentAuthor = post.InReplyToFullPost.Author.ToApiCompatProfileViewBasic(),
                } : null
            };
        }
        public static StrongRef GetPostStrongRef(string did, string rkey)
        {
            return GetStrongRef(GetPostUri(did, rkey));
        }
        public static StrongRef GetStrongRef(ATUri uri)
        {
            return new StrongRef
            {
                Uri = uri,
                Cid = GetSyntheticCid(uri)
            };
        }

        public static string GetSyntheticCid(ATUri uri)
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

        public static ProfileView ToApiCompatProfileView(this BlueskyProfile profile)
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
                Associated = ToApiCompatProfileAssociated(fullProfile)
            };
        }

        private static ProfileAssociated ToApiCompatProfileAssociated(BlueskyFullProfile fullProfile)
        {
            return new ProfileAssociated
            {
                 Lists = fullProfile.HasLists ? 1 : 0,
                 Feedgens = fullProfile.HasFeeds ? 1 : 0,
                 Labeler = fullProfile.Profile.DidDoc?.AtProtoLabeler != null,
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

        public static GeneratorView ToApiCompatGeneratorView(this BlueskyFeedGenerator feed)
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
                Creator = feed.Author.ToApiCompatProfileView(),
            };
        }

        public static ListView ToApiCompatListView(BlueskyList x)
        {
            return new ListView
            {
                 Avatar = x.AvatarUrl,
                 Cid = GetSyntheticCid(x.AtUri),
                 Creator = ToApiCompatProfileView(x.Moderator!),
                 Description = x.Description,
                 DescriptionFacets = ToApiCompatFacets(x.DescriptionFacets, Encoding.UTF8.GetBytes(x.Description ?? string.Empty)),
                 IndexedAt = DateTime.UtcNow,
                 Uri = x.AtUri,
                 Purpose = ToApiCompatListPurpose(x.Data?.Purpose ?? default),
                 Name = x.DisplayNameOrFallback,
                 Viewer = ToApiCompatListViewer(x),
            };
        }

        private static ListViewerState ToApiCompatListViewer(BlueskyList x)
        {
            return new ListViewerState()
            {
                 //Blocked = x.Mode == ModerationBehavior.Block ? "dummy",
                 Blocked = null,
                 Muted = x.Mode == ModerationBehavior.Mute,
            };
        }

        public static string ToApiCompatListPurpose(ListPurposeEnum listPurposeEnum)
        {
            return listPurposeEnum switch
            {
                ListPurposeEnum.Curation => "app.bsky.graph.defs#curatelist",
                ListPurposeEnum.Moderation => "app.bsky.graph.defs#modlist",
                ListPurposeEnum.Reference => "app.bsky.graph.defs#referencelist",
                _ => "app.bsky.graph.defs#curatelist"
            };
        }

        public static ListItemView ToApiCompatToListItemView(BlueskyProfile x, string moderatorDid)
        {
            return new ListItemView
            {
                Subject = ToApiCompatProfileView(x),
                Uri = new ATUri(moderatorDid + "/app.bsky.graph.listitem/" + x.RelationshipRKey.ToString()),
            };
        }

        public static async Task<T> RequestBodyToATObjectAsync<T>(Stream requestBody) where T: IJsonEncodable<T>
        {
            using var reader = new StreamReader(requestBody);
            var json = await reader.ReadToEndAsync();
            return T.FromJson(json);
        }

        public static Generator ToApiCompatGenerator(BlueskyFeedGenerator feed)
        {
            return new Generator
            {
                  DisplayName = feed.DisplayNameOrFallback,
                  Description = feed.Data?.Description,
                  DescriptionFacets = ToApiCompatFacets(feed.Data?.DescriptionFacets, Encoding.UTF8.GetBytes(feed.Data?.Description ?? string.Empty)),
                  Did = feed.Data?.ImplementationDid != null ? new ATDid(feed.Data.ImplementationDid) : null,
                  Avatar = new Blob 
                  { 
                      Ref = feed.Data?.AvatarCid != null ? new ATLinkRef(ATCid.Read(feed.Data.AvatarCid)) : null
                  }
            };
        }
    }
}

