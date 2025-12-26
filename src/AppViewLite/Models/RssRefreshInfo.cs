using AppViewLite.PluggableProtocols.Rss;
using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    [ProtoContract]
    public class RssRefreshInfo
    {
        [ProtoMember(1)] public DateTime LastRefreshAttempt;
        [ProtoMember(2)] public HttpStatusCode LastHttpStatus;
        [ProtoMember(3)] public int XmlPostCount;
        [ProtoMember(4)] public DateTime? XmlOldestPost;
        [ProtoMember(5)] public DateTime? XmlNewestPost;
        [ProtoMember(6)] public string? RedirectsTo;
        [ProtoMember(7)] public DateTime? LastSuccessfulRefresh;
        [ProtoMember(8)] public DateTime FirstRefresh;
        [ProtoMember(9)] public HttpRequestError LastHttpError;
        [ProtoMember(10)] public string? OtherException;
        [ProtoMember(11)] public string? HttpLastETag;
        [ProtoMember(12)] public DateTime? HttpLastModified;
        [ProtoMember(14)] public DateTime? HttpLastDate;
        [ProtoMember(15)] public int HttpLastModifiedTzOffset;
        [ProtoMember(16)] public byte[]? FaviconUrl;
        [ProtoMember(17)] public bool DidAttemptFaviconRetrieval;
        [ProtoMember(18)] public bool IsTwitter;

        public void MakeUtc()
        {
            ExtensionMethods.MakeUtc(ref LastRefreshAttempt);
            ExtensionMethods.MakeUtc(ref XmlOldestPost);
            ExtensionMethods.MakeUtc(ref XmlNewestPost);
            ExtensionMethods.MakeUtc(ref LastSuccessfulRefresh);
            ExtensionMethods.MakeUtc(ref FirstRefresh);
            ExtensionMethods.MakeUtc(ref HttpLastModified);
        }

        public BlueskyProfile? BlueskyProfile;

        public string? RssErrorMessage
        {
            get
            {
                if (LastHttpError == RssProtocol.TimeoutError)
                    return "RSS fetch error: Timeout";
                if (OtherException != null)
                    return "RSS fetch error: " + OtherException;
                if (LastHttpError != default)
                    return "RSS fetch error: " + LastHttpError;
                if (LastHttpStatus != default && ((int)LastHttpStatus < 200 || (int)LastHttpStatus > 299))
                    return "RSS fetch error: HTTP " + ((int)LastHttpStatus) + " " + LastHttpError;
                return null;
            }
        }

    }
}

