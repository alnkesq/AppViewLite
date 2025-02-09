using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.PluggableProtocols.Nostr
{

    // https://github.com/nostr-protocol/nips
    // Get-Clipboard | ConvertFrom-Csv -Delimiter "`t" -Header Kind,Name | %{ $_.Name.Trim().Replace(' ', '_').Replace('-', '_') + " = " + $_.Kind + "," } | Set-Clipboard
    public enum NostrEventKind
    {
        User_Metadata = 0,
        Short_Text_Note = 1,
        Recommend_Relay = 2,
        Follows = 3,
        Encrypted_Direct_Messages = 4,
        Event_Deletion_Request = 5,
        Repost = 6,
        Reaction = 7,
        Badge_Award = 8,
        Chat_Message = 9,
        Group_Chat_Threaded_Reply = 10,
        Thread = 11,
        Group_Thread_Reply = 12,
        Seal = 13,
        Direct_Message = 14,
        Generic_Repost = 16,
        Reaction_to_a_website = 17,
        Picture = 20,
        Video_Event = 21,
        Short_form_Portrait_Video_Event = 22,
        Channel_Creation = 40,
        Channel_Metadata = 41,
        Channel_Message = 42,
        Channel_Hide_Message = 43,
        Channel_Mute_User = 44,
        Chess_PGN = 64,
        Merge_Requests = 818,
        Poll_Response = 1018,
        Bid = 1021,
        Bid_confirmation = 1022,
        OpenTimestamps = 1040,
        Gift_Wrap = 1059,
        File_Metadata = 1063,
        Poll = 1068,
        Comment = 1111,
        Live_Chat_Message = 1311,
        Patches = 1617,
        Issues = 1621,
        Replies = 1622,
        Status = 1630 - 1633,
        Problem_Tracker = 1971,
        Reporting = 1984,
        Label = 1985,
        Relay_reviews = 1986,
        AI_Embeddings_Vector_lists = 1987,
        Torrent = 2003,
        Torrent_Comment = 2004,
        Coinjoin_Pool = 2022,
        Community_Post_Approval = 4550,
        Job_Request = 5000 - 5999,
        Job_Result = 6000 - 6999,
        Job_Feedback = 7000,
        Reserved_Cashu_Wallet_Tokens = 7374,
        Cashu_Wallet_Tokens = 7375,
        Cashu_Wallet_History = 7376,
        Group_Control_Events = 9000 - 9030,
        Zap_Goal = 9041,
        Nutzap = 9321,
        Tidal_login = 9467,
        Zap_Request = 9734,
        Zap = 9735,
        Highlights = 9802,
        Mute_list = 10000,
        Pin_list = 10001,
        Relay_List_Metadata = 10002,
        Bookmark_list = 10003,
        Communities_list = 10004,
        Public_chats_list = 10005,
        Blocked_relays_list = 10006,
        Search_relays_list = 10007,
        User_groups = 10009,
        Private_event_relay_list = 10013,
        Interests_list = 10015,
        Nutzap_Mint_Recommendation = 10019,
        User_emoji_list = 10030,
        Relay_list_to_receive_DMs = 10050,
        User_server_list = 10063,
        File_storage_server_list = 10096,
        Wallet_Info = 13194,
        Cashu_Wallet_Event = 17375,
        Lightning_Pub_RPC = 21000,
        Client_Authentication = 22242,
        Wallet_Request = 23194,
        Wallet_Response = 23195,
        Nostr_Connect = 24133,
        Blobs_stored_on_mediaservers = 24242,
        HTTP_Auth = 27235,
        Follow_sets = 30000,
        Generic_lists = 30001,
        Relay_sets = 30002,
        Bookmark_sets = 30003,
        Curation_sets = 30004,
        Video_sets = 30005,
        Kind_mute_sets = 30007,
        Profile_Badges = 30008,
        Badge_Definition = 30009,
        Interest_sets = 30015,
        Create_or_update_a_stall = 30017,
        Create_or_update_a_product = 30018,
        Marketplace_UI_UX = 30019,
        Product_sold_as_an_auction = 30020,
        Long_form_Content = 30023,
        Draft_Long_form_Content = 30024,
        Emoji_sets = 30030,
        Modular_Article_Header = 30040,
        Modular_Article_Content = 30041,
        Release_artifact_sets = 30063,
        Application_specific_Data = 30078,
        App_curation_sets = 30267,
        Live_Event = 30311,
        User_Statuses = 30315,
        Slide_Set = 30388,
        Classified_Listing = 30402,
        Draft_Classified_Listing = 30403,
        Repository_announcements = 30617,
        Repository_state_announcements = 30618,
        Wiki_article = 30818,
        Redirects = 30819,
        Draft_Event = 31234,
        Link_Set = 31388,
        Feed = 31890,
        Date_Based_Calendar_Event = 31922,
        Time_Based_Calendar_Event = 31923,
        Calendar = 31924,
        Calendar_Event_RSVP = 31925,
        Handler_recommendation = 31989,
        Handler_information = 31990,
        Software_Application = 32267,
        Community_Definition = 34550,
        Peer_to_peer_Order_events = 38383,
        Group_metadata_events = 39000 - 9,
    }

    public static class NostrTags
    {
        // part 1. single letters:
        // https://github.com/nostr-protocol/nips
        // Get-Clipboard | ConvertFrom-Csv -Delimiter "`t" -Header Letter,Name | %{ "public const string " + $_.Name.Replace(',','').Replace('(', '_').Replace(')', '_').Replace(' ', '_').Replace('-', '_').Replace('__', '_').Trim('_') + " = """ + $_.Letter.Trim() + """;" }  | Set-Clipboard
        public const string coordinates_to_an_event = "a";
        public const string root_address = "A";
        public const string identifier = "d";
        public const string event_id_hex = "e";
        public const string root_event_id = "E";
        public const string currency_code = "f";
        public const string geohash = "g";
        public const string group_id = "h";
        public const string external_identity = "i";
        public const string root_external_identity = "I";
        public const string kind = "k";
        public const string root_scope = "K";
        public const string label_label_namespace = "l";
        public const string label_namespace = "L";
        public const string MIME_type = "m";
        public const string pubkey_hex_lowercase = "p";
        public const string pubkey_hex_uppercase = "P";
        public const string q = "q";
        public const string a_reference_URL_etc = "r";
        public const string relay_url = "r";
        public const string status = "s";
        public const string hashtag = "t";
        public const string url = "u";
        public const string infohash = "x";
        public const string platform = "y";
        public const string order_number = "z";
        public const string dash = "-";

        // part 2, readable words
        // public const string Get-Clipboard | ConvertFrom-Csv -Delimiter "`t" -Header Letter,Name | %{ "public const string " + $_.Letter.Trim() + " = """ + $_.Letter.Trim() + """;" }   | Set-Clipboard
        public const string alt = "alt";
        public const string amount = "amount";
        public const string bolt11 = "bolt11";
        public const string challenge = "challenge";
        public const string client = "client";
        public const string clone = "clone";
        public const string content_warning = "content-warning";
        public const string delegation = "delegation";
        public const string description = "description";
        public const string emoji = "emoji";
        public const string encrypted = "encrypted";
        public const string expiration = "expiration";
        public const string file = "file";
        public const string goal = "goal";
        public const string image = "image";
        public const string imeta = "imeta";
        public const string lnurl = "lnurl";
        public const string location = "location";
        public const string name = "name";
        public const string nonce = "nonce";
        public const string preimage = "preimage";
        public const string price = "price";
        public const string proxy = "proxy";
        public const string published_at = "published_at";
        public const string relay = "relay";
        public const string relays = "relays";
        public const string server = "server";
        public const string subject = "subject";
        public const string summary = "summary";
        public const string thumb = "thumb";
        public const string title = "title";
        public const string tracker = "tracker";
        public const string web = "web";
        public const string zap = "zap";
    }


    public class NostrProfileJson
    {
        public string picture;
        public string banner;
        public string about;
        public string name;
        public string website;
    }

}

