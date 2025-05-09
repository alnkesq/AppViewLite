using ProtoBuf;
using AppViewLite.Numerics;
using DuckDbSharp;
using DuckDbSharp.Types;
using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Collections.Generic;
using System.Text;

namespace AppViewLite.Models
{
    [ProtoContract]
    public class DidDocProto
    {
        [ProtoMember(1)] public string? BskySocialUserName;
        [ProtoMember(2)] public int? PdsId;
        [ProtoMember(3)] public string? CustomDomain;
        [ProtoMember(4)] public DateTime Date;
        [ProtoMember(5)] public string? Pds;
        [ProtoMember(6)] public string[]? MultipleHandles;

        [ProtoMember(7)] public string? TrustedDid;
        [DuckDbInclude] public DuckDbUuid PlcAsUInt128;
        [ProtoMember(9)] public string[]? OtherUrls;
        [ProtoMember(10)] public string? AtProtoLabeler;
        [ProtoMember(11)] public ushort? EarliestDateApprox16;

        public int OriginalPayloadApproximateSize;

        public bool IsSpam
        {
            get
            {
                
                // thousands of entries every 2-3 minutes since early April 2025
                var isSpam = 
                    Pds is "https://pds.trump.com" or "https://plc.surge.sh/gallery" ||
                    AllHandlesAndDomains.Any(x => x != null && IsSpamHandleOrDomain(x)) ||
                    (OtherUrls is { Length: >= 1 } && OtherUrls.Any(IsSpamOtherUrl)) ||
                    (Pds == null && Date >= new DateTime(2025, 3, 31, 0, 0, 0, DateTimeKind.Utc) && Date <= new DateTime(2025, 4, 3, 0, 0, 0, DateTimeKind.Utc))
                ;
                if (!isSpam)
                {
                    if (OriginalPayloadApproximateSize > 200)
                    { 
                    
                    }
                }
                return isSpam;
            }
        }

        private static bool IsSpamOtherUrl(string url)
        {
            if (url.Length >= 50 && !url.AsSpan().ContainsAny(':', '.')) return true;

            if (Uri.TryCreate(url, UriKind.Absolute, out var u))
            {
                if (u.Scheme == Uri.UriSchemeHttp || u.Scheme == Uri.UriSchemeHttps)
                {
                    return IsSpamHandleOrDomain(u.Host);
                }
            }
            return false;
        }

        private static bool IsSpamHandleOrDomain(string domain)
        {
            if (!domain.Contains('.')) return true;
            if (domain.EndsWith(".a.co", StringComparison.Ordinal)) return true;
            if (domain.EndsWith(".flipboard.com.ap.brid.gy", StringComparison.Ordinal)) return true;
            if (domain.EndsWith(".awakari.com.ap.brid.gy", StringComparison.Ordinal)) return true;
            if (domain.Contains('_')) return true;
            return false;
        }

        public IEnumerable<string?> AllHandlesAndDomains => [GetDomainFromPds(Pds), Handle, .. MultipleHandles ?? []];

        public static string? GetDomainFromPds(string? pds)
        {
            if (pds == null) return null;
            if (!pds.Contains('/')) return pds;
            var url = StringUtils.TryParseUri(pds);
            if (url == null || url.Scheme != Uri.UriSchemeHttps) return null;
            return url.Host;
        }

        public string? Handle => (CustomDomain ?? (BskySocialUserName != null ? BskySocialUserName + ".bsky.social" : null)) ?? MultipleHandles?.FirstOrDefault();


        public bool HasHandle(string normalizedHandle)
        {
            ArgumentNullException.ThrowIfNull(normalizedHandle);
            if (MultipleHandles != null) return MultipleHandles.Any(x => StringUtils.NormalizeHandle(x) == normalizedHandle);
            var h = Handle;
            return h != null && StringUtils.NormalizeHandle(h) == normalizedHandle;
        }

        public override string ToString()
        {
            return $"{Date} {Handle}: {Pds ?? PdsId.ToString()}";
        }

        public byte[] SerializeToBytes()
        {
            if (Pds != null) throw new InvalidOperationException("DidDocProto.SerializeToBytes: Pds should already be interned.");

            using var ms = new MemoryStream();
            using var bw = new BinaryWriter(ms);


            DidDocEncoding format = default;
            bw.Write((byte)format); // will be overwritten later

            if (MultipleHandles != null || OtherUrls != null || AtProtoLabeler != null)
            {
                format = DidDocEncoding.Proto;
                ProtoBuf.Serializer.Serialize(ms, this);
            }
            else
            {
                var date = Date;
                if (date < ApproximateDateTime32.MinValueAsDateTime)
                    date = ApproximateDateTime32.MinValueAsDateTime;
                bw.Write(Unsafe.BitCast<ApproximateDateTime32, uint>((ApproximateDateTime32)date));
                if (PdsId != null)
                {
                    format |= DidDocEncoding.HasPds;
                    bw.Write7BitEncodedInt(PdsId.Value);
                }
                if (EarliestDateApprox16 != null)
                {
                    format |= DidDocEncoding.HasEarliestDate;
                    bw.Write(EarliestDateApprox16.Value);
                }
                if (CustomDomain != null)
                {
                    format |= DidDocEncoding.HasCustomDomain;
                    bw.Write(Encoding.UTF8.GetBytes(CustomDomain)!);
                }
                else if (BskySocialUserName != null)
                {
                    format |= DidDocEncoding.HasBskySocialUserName;

                    bw.Write(Encoding.UTF8.GetBytes(BskySocialUserName)!);

                }
            }
            var result = ms.ToArray();
            result[0] = (byte)format;
            return result;
        }



        public static DidDocProto? DeserializeFromBytes(ReadOnlySpan<byte> bytes, bool onlyIfProtobufEncoding = false)
        {
            using var br = new BinaryReader(new MemoryStream(bytes.ToArray()));
            var format = (DidDocEncoding)br.ReadByte();
            if (format == DidDocEncoding.Proto)
            {
                var proto = ProtoBuf.Serializer.Deserialize<DidDocProto>(br.BaseStream);
                return proto;
            }
            if (onlyIfProtobufEncoding) return null;
            var result = new DidDocProto();
            result.Date = Unsafe.BitCast<uint, ApproximateDateTime32>(br.ReadUInt32());
            if ((format & DidDocEncoding.HasPds) != 0)
            {
                result.PdsId = br.Read7BitEncodedInt();
            }
            if ((format & DidDocEncoding.HasEarliestDate) != 0)
            {
                result.EarliestDateApprox16 = br.ReadUInt16();
            }
            if ((format & DidDocEncoding.HasCustomDomain) != 0)
            {
                var length = (int)(bytes.Length - br.BaseStream.Position);
                result.CustomDomain = Encoding.UTF8.GetString(br.ReadBytes(length));
            }
            if ((format & DidDocEncoding.HasBskySocialUserName) != 0)
            {
                var length = (int)(bytes.Length - br.BaseStream.Position);
                result.BskySocialUserName = Encoding.UTF8.GetString(br.ReadBytes(length));
            }

            return result;
        }


        [Flags]
        enum DidDocEncoding : byte
        {
            None,
            HasBskySocialUserName = 1,
            HasCustomDomain = 2,
            HasPds = 4,
            HasEarliestDate = 8,

            Proto = HasBskySocialUserName | DidDocEncoding.HasCustomDomain,
        }
    }
}

