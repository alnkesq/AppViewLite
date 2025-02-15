using Google.Protobuf.WellKnownTypes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

#nullable disable

namespace AppViewLite.PluggableProtocols.ActivityPub
{
    // Visual Studio -> Paste JSON as Classes
    // Get-Clipboard | % { $_.Replace(' { get; set; }', ';') } | Sort-Object { $_.Substring($_.LastIndexOf(' ')) } | Select-Object -Unique | Set-Clipboard

    public class ActivityPubPostJson
    {
        public ActivityPubAccountJson account;
        public ActivityPubCardJson card;
        public string content;
        public DateTime created_at;
        public string? in_reply_to_account_id;
        public object? conversation_id;
        public string language;
        public ActivityPubMediaAttachmentJson[] media_attachments;
        public ActivityPubMentionJson[] mentions;
        public ActivityPubPostJson reblog;
        public string spoiler_text;
        public ActivityPubTagJson[] tags;
        public string url;
        public ActivityPubEmojiJson[] emojis;
    }
    public class ActivityPubMentionJson
    {
        public string acct;
        public string id;
        public string url;
        public string username;
    }
    public class ActivityPubMediaAttachmentJson
    {
        public string description;
        public string preview_url;
        public string remote_url;
        public string type;
    }
    public class ActivityPubAccountJson
    {
        public string acct;
        public string avatar_static;
        public string avatar;
        public bool? bot;
        public DateTime created_at;
        public bool? discoverable;
        public string display_name;
        public ActivityPubEmojiJson[] emojis;
        public ActivityPubAccountFieldJson[] fields;
        public long? followers_count;
        public long? following_count;
        public string fqn;
        public bool? group;
        public string header_static;
        public string header;
        public string id;
        public string last_status_at;
        public bool? locked;
        public bool? noindex;
        public ActivityPubNostrJson? nostr;
        public string? note;
        public ActivityPubRoleJson[] roles;
        public long? statuses_count;
        public string? uri;
        public string? url;
        public string? username;
    }
    public class ActivityPubNostrJson
    {
    }
    public class ActivityPubTagJson
    {
        public string name;
    }
    public class ActivityPubAccountFieldJson
    {
        public string name;
        public string value;
        public DateTime? verified_at;
    }


    public class ActivityPubCardJson
    {
        public string author_name;
        public string author_url;
        public object[] authors;
        public string blurhash;
        public string description;
        public string embed_url;
        public int height;
        public string html;
        public string image_description;
        public string image;
        public string language;
        public string provider_name;
        public string provider_url;
        public DateTime? published_at;
        public string title;
        public string type;
        public string url;
        public int width;
    }


    public class ActivityPubRoleJson
    {
        public string color;
        public string id;
        public string name;
    }

    public class ActivityPubEmojiJson
    {
        public string shortcode;
        public string static_url;
        public string url;
        public bool? visible_in_picker;
    }

}

