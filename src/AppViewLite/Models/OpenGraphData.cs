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
    public class OpenGraphData
    {
        [ProtoMember(1)] public byte[]? ExternalTitleBpe;
        [ProtoMember(2)] public byte[]? ExternalDescriptionBpe;
        [ProtoMember(3)] public byte[]? ExternalUrlBpe;
        [ProtoMember(4)] public byte[]? ExternalThumbnailUrlBpe;
        [ProtoMember(5)] public DateTime DateFetched;
        [ProtoMember(6)] public HttpRequestError? HttpError;
        [ProtoMember(7)] public HttpStatusCode HttpStatusCode;
        [ProtoMember(8)] public string? Error;

        [ProtoMember(10)] public string? ExternalTitle;
        [ProtoMember(11)] public string? ExternalDescription;
        [ProtoMember(12)] public string? ExternalUrl;
        [ProtoMember(13)] public string? ExternalThumbnailUrl;
    }
}

