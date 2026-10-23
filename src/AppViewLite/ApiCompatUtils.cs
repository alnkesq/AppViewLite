using AppBsky.Actor;
using AppBsky.Embed;
using AppBsky.Feed;
using AppBsky.Graph;
using AppBsky.Richtext;
using AppViewLite.Models;
using CarpaNet;
using ComAtproto.Repo;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace AppViewLite
{
    public static class ApiCompatUtils
    {
        public static DefsThreadViewPost ToApiCompatThreadViewPost(this BlueskyPost post, RequestContext ctx, BlueskyPost? rootPost, DefsThreadViewPost? parent = null)
        {
            return new DefsThreadViewPost
            {
                Post = post.ToApiCompatPostView(ctx, rootPost),
                Parent = parent,
            };
        }
        public static DefsPostView ToApiCompatPostView(this BlueskyPost post, RequestContext ctx, BlueskyPost? rootPost = null)
        {
            var aturi = GetPostUri(post.Author.Did, post.RKey);

            return new DefsPostView
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

        private static IDefsPostViewEmbed? ToApiCompatEmbedView(BlueskyPost post)
        {

            /*
             Must return one of:
                ViewImages
                ViewVideo
                ViewExternal
                ViewRecordDef
                ViewRecordWithMedia
             */
            IDefsPostViewEmbed? embed = null;
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
                    embed = new ImagesView
                    {
                        Images = post.Data.Media.Select(x => ToApiCompatViewImage(x, post.Author)).ToList()
                    };
                }
            }

            if (post.Data?.ExternalUrl != null)
            {
                embed = new ExternalView
                {
                    External = new ExternalViewExternal
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
                var recordView = new RecordView { Record = ToApiCompatRecordView(post.QuotedPost) };
                if (embed != null)
                {
                    embed = new RecordWithMediaView { Media = (IRecordWithMediaViewMedia)embed, Record = recordView };
                }
                else
                {
                    embed = recordView;
                }
            }
            return embed;
        }

        private static IPostEmbed? ToApiCompatEmbed(BlueskyPost post)
        {
            /*
             Must return one of:
                EmbedImages
                EmbedVideo
                EmbedExternal
                EmbedRecord
                RecordWithMedia
             */
            IPostEmbed? embed = null;
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
                    embed = new Images
                    {
                        ImagesValue = post.Data.Media.Select(x => ToApiCompatImage(x)).ToList()
                    };
                }
            }

            if (post.Data?.ExternalUrl != null)
            {
                embed = new External
                {
                    ExternalValue = new ExternalExternal
                    {
                        Description = post.Data.ExternalDescription ?? string.Empty,
                        Title = post.Data.ExternalTitle ?? string.Empty,
                        Uri = post.Data.ExternalUrl,
                        Thumb = post.Data.ExternalThumbCid != null ? new ATBlob(ATCid.FromBytes(post.Data.ExternalThumbCid), "image/jpeg", 0) : null
                    }
                };
            }

            if (post.QuotedPost != null)
            {
                var record = new Record { RecordValue = GetPostStrongRef(post.QuotedPost!.Did, post.QuotedPost.RKey) };
                if (embed != null)
                {
                    return new RecordWithMedia { Media = (IRecordWithMediaMedia)embed, Record = record };
                }
                else
                {
                    embed = record;
                }
            }
            return embed;
        }


        private static AppBsky.Feed.DefsViewerState ToApiCompatPostViewerState(BlueskyPost post, RequestContext ctx)
        {
            return new AppBsky.Feed.DefsViewerState
            {
                Like = post.IsLikedBySelf != null ? new ATUri("at://" + ctx.UserContext.Did + "/" + Like.RecordType + "/" + post.IsLikedBySelf.Value) : null,
                Repost = post.IsRepostedBySelf != null ? new ATUri("at://" + ctx.UserContext.Did + "/" + Repost.RecordType + "/" + post.IsRepostedBySelf.Value) : null,
            };
        }

        private static RecordViewRecord ToApiCompatRecordView(BlueskyPost post)
        {
            var aturi = post.AtUri;
            return new RecordViewRecord
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
                Embeds = ToApiCompatEmbedView(post) is { } v ? [(IRecordViewRecordEmbeds)v] : [],
            };
        }

        public static JsonElement ToApiCompatPost(BlueskyPost post)
        {
            return new Post
            {
                CreatedAt = post.Date,
                Text = post.Data != null ? (post.Data.Error != null ? "[" + post.Data.Error + "]" : post.Data.Text) : "[Post data not loaded yet, please refresh page or post]",
                Reply = post.InReplyToPostId != null ? new PostReplyRef
                {
                    Parent = GetPostStrongRef(post.InReplyToUser!.Did, post.Data!.InReplyToRKeyString!),
                    Root = GetPostStrongRef(post.RootPostDid!, post.RootPostId.PostRKey.ToString()!)
                } : null,
                Facets = ToApiCompatFacets(post.Data?.Facets, post.Data?.GetUtf8IfNeededByCompactFacets()),
                Langs = post.Data?.Language != null ? [ToApiCompatLanguage(post.Data.Language.Value)] : null,
                Embed = ToApiCompatEmbed(post)
            }.ToJson();
        }

        private static string ToApiCompatLanguage(LanguageEnum language)
        {
            return language.ToString().Replace('_', '-');
        }


        private static ImagesImage ToApiCompatImage(BlueskyMediaData x)
        {
            return new ImagesImage
            {
                Alt = x.AltText ?? string.Empty,
                Image = new ATBlob
                {
                    Ref = ATCid.FromBytes(x.Cid),
                },
                AspectRatio = GetDefaultAspectRatio()
            };
        }

        private static DefsAspectRatio GetDefaultAspectRatio()
        {
            // best guess
            return new DefsAspectRatio
            {
                Width = 1600,
                Height = 900
            };
        }

        private static ImagesViewImage ToApiCompatViewImage(BlueskyMediaData x, BlueskyProfile author)
        {
            return new ImagesViewImage
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
                IFacetFeatures? feature = null;
                if (x.Did != null)
                {
                    feature = new FacetMention { Did = new ATDid(x.Did) };
                }
                else if (x.IsLink)
                {
                    feature = new FacetLink
                    {
                        Uri = x.GetLink(utf8)!
                    };
                }

                if (feature == null) return null;
                return new Facet
                {
                    Index = new FacetByteSlice { ByteStart = x.Start, ByteEnd = x.Start + x.Length },
                    Features = [feature]
                };
            }).WhereNonNull().ToList();
        }

        public static DefsFeedViewPost ToApiCompatFeedViewPost(this BlueskyPost post, RequestContext ctx)
        {
            return new DefsFeedViewPost
            {
                Post = post.ToApiCompatPostView(ctx, null),
                Reply = post.InReplyToFullPost != null ? new DefsReplyRef
                {
                    Parent = (IDefsReplyRefParent)post.InReplyToFullPost.ToApiCompatFeedViewPost(ctx),
                    Root = (IDefsReplyRefRoot)post.RootFullPost!.ToApiCompatFeedViewPost(ctx),
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
            var c = new ATCid("bafyreiaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").ToBytesV2();
            var preservePrefix = 4;
            c.AsSpan(preservePrefix).Clear();
            var hash = SHA256.HashData(Encoding.UTF8.GetBytes(uri.ToString()));
            hash.CopyTo(c.AsSpan(preservePrefix));
            var q = ATCid.FromBytes(c);
            return q.ToString();
        }

        private static ATUri GetPostUri(string did, string rkey)
        {
            return new ATUri("at://" + did + "/app.bsky.feed.post/" + rkey);
        }

        private readonly static DateTime DummyDate = new DateTime(2023, 2, 1, 0, 0, 0, DateTimeKind.Utc);

        public static DefsProfileView ToApiCompatProfileView(this BlueskyProfile profile)
        {
            return new AppBsky.Actor.DefsProfileView
            {
                DisplayName = profile.DisplayNameOrFallback,
                Labels = [],
                CreatedAt = DummyDate,
                Avatar = GetAvatarUrl(profile),
                Did = new ATDid(profile.Did),
                Handle = GetHandle(profile),
                Description = profile.BasicData?.Description,
                Viewer = new AppBsky.Actor.DefsViewerState
                {
                    Muted = false,
                    BlockedBy = false,
                },
            };
        }

        private static ATHandle GetHandle(BlueskyProfile profile)
        {
            return new ATHandle(profile.HandleIsUncertain || profile.PossibleHandle == null ? "handle.invalid" : profile.PossibleHandle);
        }

        private static string? GetAvatarUrl(BlueskyProfile profile)
        {
            return BlueskyEnrichedApis.Instance.GetAvatarUrl(profile.Did, profile.BasicData?.AvatarCidBytes, profile.Pds);
        }
        private static string? GetAvatarUrl(BlueskyFeedGenerator feed)
        {
            return BlueskyEnrichedApis.Instance.GetAvatarUrl(feed.Did, feed.Data?.AvatarCid, feed.Author.Pds);
        }

        public static DefsProfileViewBasic ToApiCompatProfileViewBasic(this BlueskyProfile profile)
        {
            return new AppBsky.Actor.DefsProfileViewBasic
            {
                DisplayName = profile.DisplayNameOrFallback,
                Labels = [],
                CreatedAt = DummyDate,
                Avatar = GetAvatarUrl(profile),
                Did = new ATDid(profile.Did),
                Handle = GetHandle(profile),
                Viewer = new AppBsky.Actor.DefsViewerState
                {
                    Muted = false,
                    BlockedBy = false,
                },
            };
        }
        public static DefsProfileViewDetailed ToApiCompatProfileDetailed(this BlueskyFullProfile fullProfile)
        {
            var profile = fullProfile.Profile;
            return new AppBsky.Actor.DefsProfileViewDetailed
            {
                DisplayName = profile.DisplayNameOrFallback,
                Labels = [],
                CreatedAt = DummyDate,
                Avatar = GetAvatarUrl(profile),
                Did = new ATDid(profile.Did),
                Handle = GetHandle(profile),
                Description = profile.BasicData?.Description,
                Banner = BlueskyEnrichedApis.Instance.GetImageBannerUrl(profile.Did, profile.BasicData?.BannerCidBytes, profile.Pds),
                Viewer = new AppBsky.Actor.DefsViewerState
                {
                    Muted = false,
                    BlockedBy = false,
                },
                FollowersCount = fullProfile.Followers,
                FollowsCount = fullProfile.Following,
                Associated = ToApiCompatProfileAssociated(fullProfile)
            };
        }

        private static DefsProfileAssociated ToApiCompatProfileAssociated(BlueskyFullProfile fullProfile)
        {
            return new DefsProfileAssociated
            {
                Lists = fullProfile.HasLists ? 1 : 0,
                Feedgens = fullProfile.HasFeeds ? 1 : 0,
                Labeler = fullProfile.Profile.DidDoc?.AtProtoLabeler != null,
            };
        }

        private static DefsProfileAssociated CreateProfileAssociated()
        {
            return new AppBsky.Actor.DefsProfileAssociated
            {
                Lists = 0,
                Feedgens = 0,
                StarterPacks = 0,
                Labeler = false,
                Chat = new AppBsky.Actor.DefsProfileAssociatedChat
                {
                    AllowIncoming = "none"
                }
            };
        }

        public static DefsGeneratorView ToApiCompatGeneratorView(this BlueskyFeedGenerator feed)
        {
            return new DefsGeneratorView
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

        public static DefsListView ToApiCompatListView(BlueskyList x)
        {
            return new DefsListView
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

        private static DefsListViewerState ToApiCompatListViewer(BlueskyList x)
        {
            return new DefsListViewerState()
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

        public static DefsListItemView ToApiCompatToListItemView(BlueskyProfile x, string moderatorDid)
        {
            return new DefsListItemView
            {
                Subject = ToApiCompatProfileView(x),
                Uri = new ATUri(moderatorDid + "/app.bsky.graph.listitem/" + x.RelationshipRKey.ToString()),
            };
        }

        //public static async Task<T> RequestBodyToATObjectAsync<T>(Stream requestBody) where T : IJsonEncodable<T>
        //{
        //    using var reader = new StreamReader(requestBody);
        //    var json = await reader.ReadToEndAsync();
        //    return T.FromJson(json);
        //}

        public static Generator ToApiCompatGenerator(BlueskyFeedGenerator feed)
        {
            return new Generator
            {
                CreatedAt = DateTime.UtcNow,
                DisplayName = feed.DisplayNameOrFallback,
                Description = feed.Data?.Description,
                DescriptionFacets = ToApiCompatFacets(feed.Data?.DescriptionFacets, Encoding.UTF8.GetBytes(feed.Data?.Description ?? string.Empty)),
                Did = feed.Data?.ImplementationDid != null ? new ATDid(feed.Data.ImplementationDid) : null,
                Avatar = feed.Data?.AvatarCid != null ? new ATBlob
                {
                    Ref = ATCid.FromBytes(feed.Data.AvatarCid)
                } : null
            };
        }
    }
}

