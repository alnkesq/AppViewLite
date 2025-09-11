using System.Collections.Generic;

#nullable disable

namespace AppViewLite.PluggableProtocols.Reddit
{

    public class RedditApiResponse
    {
        public string kind { get; set; }
        public RedditApiData data { get; set; }
    }

    public class RedditApiData
    {
        public string after { get; set; }
        public int dist { get; set; }
        public string modhash { get; set; }
        public string geo_filter { get; set; }
        public RedditApiChild[] children { get; set; }
        public object before { get; set; }
    }

    public class RedditApiChild
    {
        public string kind { get; set; }
        public RedditApiChildData data { get; set; }
    }

    public class RedditApiChildData
    {
        public object approved_at_utc { get; set; }
        public string subreddit { get; set; }
        public string selftext { get; set; }
        public string author_fullname { get; set; }
        public bool saved { get; set; }
        public object mod_reason_title { get; set; }
        public int gilded { get; set; }
        public bool clicked { get; set; }
        public string title { get; set; }
        public object[] link_flair_richtext { get; set; }
        public string subreddit_name_prefixed { get; set; }
        public bool hidden { get; set; }
        public object pwls { get; set; }
        public object link_flair_css_class { get; set; }
        public int downs { get; set; }
        public int? thumbnail_height { get; set; }
        public object top_awarded_type { get; set; }
        public bool hide_score { get; set; }
        public string name { get; set; }
        public bool quarantine { get; set; }
        public string link_flair_text_color { get; set; }
        public float upvote_ratio { get; set; }
        public object author_flair_background_color { get; set; }
        public string subreddit_type { get; set; }
        public int ups { get; set; }
        public int total_awards_received { get; set; }
        public RedditApiMediaEmbed media_embed { get; set; }
        public int? thumbnail_width { get; set; }
        public object author_flair_template_id { get; set; }
        public bool is_original_content { get; set; }
        public object[] user_reports { get; set; }
        public object secure_media { get; set; }
        public bool is_reddit_media_domain { get; set; }
        public bool is_meta { get; set; }
        public object category { get; set; }
        public ReddditApiMediaSecureEmbed secure_media_embed { get; set; }
        public object link_flair_text { get; set; }
        public bool can_mod_post { get; set; }
        public int score { get; set; }
        public object approved_by { get; set; }
        public bool is_created_from_ads_ui { get; set; }
        public bool author_premium { get; set; }
        public string thumbnail { get; set; }
        public object edited { get; set; }
        public object author_flair_css_class { get; set; }
        public object[] author_flair_richtext { get; set; }
        public RedditApiGildings gildings { get; set; }
        public string post_hint { get; set; }
        public object content_categories { get; set; }
        public bool is_self { get; set; }
        public object mod_note { get; set; }
        public float created { get; set; }
        public string link_flair_type { get; set; }
        public object wls { get; set; }
        public object removed_by_category { get; set; }
        public object banned_by { get; set; }
        public string author_flair_type { get; set; }
        public string domain { get; set; }
        public bool allow_live_comments { get; set; }
        public string selftext_html { get; set; }
        public object likes { get; set; }
        public object suggested_sort { get; set; }
        public object banned_at_utc { get; set; }
        public string url_overridden_by_dest { get; set; }
        public object view_count { get; set; }
        public bool archived { get; set; }
        public bool no_follow { get; set; }
        public bool is_crosspostable { get; set; }
        public bool pinned { get; set; }
        public bool over_18 { get; set; }
        public RedditApiPreview preview { get; set; }
        public object[] all_awardings { get; set; }
        public object[] awarders { get; set; }
        public bool media_only { get; set; }
        public bool can_gild { get; set; }
        public bool spoiler { get; set; }
        public bool locked { get; set; }
        public object author_flair_text { get; set; }
        public object[] treatment_tags { get; set; }
        public bool visited { get; set; }
        public object removed_by { get; set; }
        public object num_reports { get; set; }
        public object distinguished { get; set; }
        public string subreddit_id { get; set; }
        public bool author_is_blocked { get; set; }
        public object mod_reason_by { get; set; }
        public object removal_reason { get; set; }
        public string link_flair_background_color { get; set; }
        public string id { get; set; }
        public bool is_robot_indexable { get; set; }
        public object report_reasons { get; set; }
        public string author { get; set; }
        public object discussion_type { get; set; }
        public int num_comments { get; set; }
        public bool send_replies { get; set; }
        public bool contest_mode { get; set; }
        public object[] mod_reports { get; set; }
        public bool author_patreon_flair { get; set; }
        public object author_flair_text_color { get; set; }
        public string permalink { get; set; }
        public bool stickied { get; set; }
        public string url { get; set; }
        public int subreddit_subscribers { get; set; }
        public double created_utc { get; set; }
        public int num_crossposts { get; set; }
        public object media { get; set; }
        public bool is_video { get; set; }

        public RedditApiGalleryData gallery_data { get; set; }
        public Dictionary<string, RedditApiMediaMetadataItem> media_metadata { get; set; }
    }

    public class RedditApiMediaEmbed
    {
    }

    public class ReddditApiMediaSecureEmbed
    {
    }

    public class RedditApiGildings
    {
    }

    public class RedditApiPreview
    {
        public RedditApiImage[] images { get; set; }
        public bool enabled { get; set; }
        public RedditApiRedditVideoPreview reddit_video_preview { get; set; }

    }


    public class RedditApiRedditVideoPreview
    {
        public int bitrate_kbps { get; set; }
        public string fallback_url { get; set; }
        public int height { get; set; }
        public int width { get; set; }
        public string scrubber_media_url { get; set; }
        public string dash_url { get; set; }
        public int duration { get; set; }
        public string hls_url { get; set; }
        public bool is_gif { get; set; }
        public string transcoding_status { get; set; }
    }


    public class RedditApiImage
    {
        public RedditApiImageSource source { get; set; }
        public RedditApiImageResolution[] resolutions { get; set; }
        public RedditApiImageVariants variants { get; set; }
        public string id { get; set; }
    }

    public class RedditApiImageSource
    {
        public string url { get; set; }
        public int width { get; set; }
        public int height { get; set; }
    }

    public class RedditApiImageResolution
    {
        public string url { get; set; }
        public int width { get; set; }
        public int height { get; set; }
    }


    public class RedditApiGalleryData
    {
        public RedditApiGalleryItem[] items { get; set; }
    }

    public class RedditApiGalleryItem
    {
        public string caption { get; set; }
        public string media_id { get; set; }
        public int id { get; set; }
    }



    public class RedditApiMediaMetadataItem
    {
        public string status { get; set; }
        public string e { get; set; }
        public string m { get; set; }
        public RedditApiMediaMetadataPreview[] p { get; set; }
        public RedditApiMediaMetadataSource s { get; set; }
        public string id { get; set; }
    }

    public class RedditApiMediaMetadataSource
    {
        public int y { get; set; }
        public int x { get; set; }
        public string u { get; set; }
        public string gif { get; set; }
        public string mp4 { get; set; }
    }

    public class RedditApiMediaMetadataPreview
    {
        public int y { get; set; }
        public int x { get; set; }
        public string u { get; set; }
    }



    public class RedditApiImageVariants
    {
        public RedditApiImageVariant obfuscated { get; set; }
        public RedditApiImageVariant gif { get; set; }
        public RedditApiImageVariant mp4 { get; set; }
        public RedditApiImageVariant nsfw { get; set; }
    }

    public class RedditApiImageVariant
    {
        public RedditApiImageVariantSource source { get; set; }
        public RedditApiImageVariantResolution[] resolutions { get; set; }
    }

    public class RedditApiImageVariantSource
    {
        public string url { get; set; }
        public int width { get; set; }
        public int height { get; set; }
    }

    public class RedditApiImageVariantResolution
    {
        public string url { get; set; }
        public int width { get; set; }
        public int height { get; set; }
    }

}

