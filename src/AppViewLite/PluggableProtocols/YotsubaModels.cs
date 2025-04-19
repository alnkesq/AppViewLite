using ProtoBuf;
using DuckDbSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace AppViewLite.PluggableProtocols.Yotsuba
{
#nullable disable warnings

    [ProtoContract]
    public class YotsubaCatalogThreadJson
    {
        [ProtoMember(1)] public long Bumplimit;
        [ProtoMember(2)] public string Capcode;
        [ProtoMember(3)] public long Closed;
        [ProtoMember(4)] public string Com;
        [ProtoMember(5)] public string Ext;
        [ProtoMember(6)] public long Filedeleted;
        [ProtoMember(7)] public string Filename;
        [ProtoMember(8)] public long Fsize;
        [ProtoMember(9)] public long H;
        [ProtoMember(10)] public long Imagelimit;
        [ProtoMember(11)] public long Images;
        [ProtoMember(12)][JsonPropertyName("last_modified")] public long LastModified;
        [ProtoMember(13)] public string Md5;
        [ProtoMember(14)] public string Name;
        [ProtoMember(15)] public long No;
        [ProtoMember(16)][DuckDbIgnore] public string Now;
        [ProtoMember(17)][JsonPropertyName("omitted_images")] public long OmittedImages;
        [ProtoMember(18)][JsonPropertyName("omitted_posts")] public long OmittedPosts;
        [ProtoMember(19)] public long Replies;
        [ProtoMember(20)] public long Resto;
        [ProtoMember(21)][JsonPropertyName("semantic_url")] public string SemanticUrl;
        [ProtoMember(22)] public long Sticky;
        [ProtoMember(23)] public string Sub;
        [ProtoMember(24)][JsonIgnore] public long Tim;
        [ProtoMember(25)] public long Time;
        [ProtoMember(26)][JsonPropertyName("tn_h")] public long TnH;
        [ProtoMember(27)][JsonPropertyName("tn_w")] public long TnW;
        [ProtoMember(28)] public string Trip;
        [ProtoMember(29)] public long W;
        [ProtoMember(30)] public string Board;
        [ProtoMember(31)][DuckDbIgnore] public string Key;
        [ProtoMember(32)] public long LastSeenInCatalog;
        [ProtoMember(33)][JsonPropertyName("board_flag")] public string BoardFlag;
        [ProtoMember(34)] public string Country;
        [ProtoMember(35)][JsonPropertyName("country_name")] public string CountryName;
        [ProtoMember(36)][JsonPropertyName("custom_spoiler")] public long CustomSpoiler;
        [ProtoMember(37)][JsonPropertyName("flag_name")] public string FlagName;
        [ProtoMember(38)] public string Id;
        [ProtoMember(39)][JsonPropertyName("m_img")] public long MImg;
        [ProtoMember(40)] public long Since4Pass;
        [ProtoMember(41)][JsonPropertyName("sticky_cap")] public long StickyCap;
        [ProtoMember(42)] public long Spoiler;



        [ProtoMember(43)][JsonIgnore] public long? Cyclical;
        [ProtoMember(44)] public string Email;
        [ProtoMember(45)] public string Embed;
        [ProtoMember(46)] public long? Locked;
        [ProtoMember(47)][JsonIgnore] public string TimString;
        [ProtoMember(48)][JsonPropertyName("extra_files")] public YotsubaExtraFileJson[] ExtraFiles;



        [ProtoMember(49)][JsonIgnore] public long? CyclicalString;
        [ProtoMember(50)][JsonPropertyName("warning_msg")] public string WarningMsg;
        [ProtoMember(51)][JsonPropertyName("ban_msg")] public string BanMsg;
        [ProtoMember(52)] public YotsubaExtraFileJson[] Files;
        [ProtoMember(53)] public long UniqueIps;

        [JsonPropertyName("tim")] public JsonValue TimObject;
        [JsonPropertyName("cyclical")] public JsonValue CyclicalObject;
    }
    [ProtoContract]
    public class YotsubaExtraFileJson
    {
        [ProtoMember(1)] public string Ext;
        [ProtoMember(2)] public string FileName;
        [ProtoMember(3)] public long? FSize;
        [ProtoMember(4)] public int? H;
        [ProtoMember(5)] public int? W;
        [ProtoMember(6)] public string Tim;
        [ProtoMember(7)] public int? Tn_H;
        [ProtoMember(8)] public int? Tn_W;
        [ProtoMember(9)] public string Md5;
        [ProtoMember(10)] public string Mime;
        [ProtoMember(11)] public string Id;
        [ProtoMember(12)][JsonIgnore] public bool? Spoiler;
        [ProtoMember(13)] public string Thumb_Path;
        [ProtoMember(14)] public string File_Path;
        [JsonPropertyName("spoiler")] public JsonValue SpoilerObject;
    }

    public class YotsubaThreadPageJson
    {
        public int Page;
        public YotsubaCatalogThreadJson[] Threads;
    }



    public class YotsubaBoardMetadataResponseJson
    {
        public YotsubaBoardMetadataJson[] boards;
    }

    public class YotsubaBoardMetadataJson
    {
        public string board;
        public string title;
        public int? ws_board;
        public int? per_page;
        public int? pages;
        public int? max_filesize;
        public int? max_webm_filesize;
        public int? max_comment_chars;
        public int? max_webm_duration;
        public int? bump_limit;
        public int? image_limit;
        public YotsubaBoardCooldownsJson? cooldowns;
        public string? meta_description;
        public int? is_archived;
        public int? spoilers;
        public int? custom_spoilers;
        public int? user_ids;
        public int? country_flags;
        public int? code_tags;
        public int? webm_audio;
        public int? min_image_width;
        public int? min_image_height;
        public int? oekaki;
        public int? sjis_tags;
        //public Board_Flags board_flags ;
        public int? text_only;
        public int? require_subject;
        public int? math_tags;
    }

    public class YotsubaBoardCooldownsJson
    {
        public int? threads;
        public int? replies;
        public int? images;
    }


}

