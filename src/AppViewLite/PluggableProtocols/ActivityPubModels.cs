using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.PluggableProtocols.ActivityPub
{

    public class ActivityPubPostJson
    {
        public string? in_reply_to_account_id;
        public ActivityPubTag[] tags;
        public string spoiler_text;
        public ActivityPubAccountJson account;
        public string url;
        public DateTime created_at;
        public ActivityPubPostJson reblog;
        public string content;
        public ActivityPubMediaAttachmentJson[] media_attachments;
        public string language;
        public ActivityPubMentionJson[] mentions;
        public ActivityPubCard card;
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
        public string fqn;
        public string acct;
        public string display_name;
        public long? statuses_count;
        public long? following_count;
        public long? followers_count;
        public ActivityPubAccountField[] fields;
        public string? note;
        public string? created_at;
        public string? username;
        public string? url;
        public ActivityPubNostr? nostr;
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
        public string url;
        public string title;
        public string description;
        public string language;
        public string type;
        public string author_name;
        public string author_url;
        public string provider_name;
        public string provider_url;
        public string html;
        public int width;
        public int height;
        public string image;
        public string image_description;
        public string embed_url;
        public string blurhash;
        public DateTime? published_at;
        public object[] authors;
    }


}

