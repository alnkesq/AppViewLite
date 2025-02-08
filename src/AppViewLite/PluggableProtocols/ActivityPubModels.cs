using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.PluggableProtocols.ActivityPub
{
    // Visual Studio -> Paste JSON as Classes
    // Get-Clipboard | % { $_.Replace(' { get; set; }', ';') } | Sort-Object { $_.Substring($_.LastIndexOf(' ')) } | Select-Object -Unique | Set-Clipboard

    public class ActivityPubPostJson
    {
        public ActivityPubAccountJson account;
        public ActivityPubCard card;
        public string content;
        public DateTime created_at;
        public string? in_reply_to_account_id;
        public string language;
        public ActivityPubMediaAttachmentJson[] media_attachments;
        public ActivityPubMentionJson[] mentions;
        public ActivityPubPostJson reblog;
        public string spoiler_text;
        public ActivityPubTag[] tags;
        public string url;
    }
    public class ActivityPubMentionJson
    {
        public string acct;
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
        public string? created_at;
        public string display_name;
        public ActivityPubAccountField[] fields;
        public long? followers_count;
        public long? following_count;
        public string fqn;
        public ActivityPubNostr? nostr;
        public string? note;
        public long? statuses_count;
        public string? url;
        public string? username;
    }
    public class ActivityPubNostr
    {
    }
    public class ActivityPubTag
    {
        public string name;
    }
    public class ActivityPubAccountField
    {
        public string name;
        public string value;
        public DateTime? verified_at;
    }


    public class ActivityPubCard
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




}

