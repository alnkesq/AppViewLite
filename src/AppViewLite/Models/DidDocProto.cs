using ProtoBuf;
using AppViewLite.Numerics;
using DuckDbSharp;
using DuckDbSharp.Types;
using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;

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
            if (Pds != null) throw new InvalidOperationException();

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
                if (CustomDomain != null)
                {
                    format |= DidDocEncoding.HasCustomDomain;
                    bw.Write(BlueskyRelationships.CompressBpe(CustomDomain));
                }
                else if (BskySocialUserName != null)
                {
                    format |= DidDocEncoding.HasBskySocialUserName;
                    
                    bw.Write(BlueskyRelationships.CompressBpe(BskySocialUserName));
                    
                }
            }
            var result = ms.ToArray();
            result[0] = (byte)format;
            return result;
        }



        public static DidDocProto DeserializeFromBytes(ReadOnlySpan<byte> bytes)
        {
            using var br = new BinaryReader(new MemoryStream(bytes.ToArray()));
            var format = (DidDocEncoding)br.ReadByte();
            if (format == DidDocEncoding.Proto)
            {
                var proto = ProtoBuf.Serializer.Deserialize<DidDocProto>(br.BaseStream);
                return proto;
            }
            var result = new DidDocProto();
            result.Date = Unsafe.BitCast<uint, ApproximateDateTime32>(br.ReadUInt32());
            if ((format & DidDocEncoding.HasPds) != 0)
            {
                result.PdsId = br.Read7BitEncodedInt();
            }
            if ((format & DidDocEncoding.HasCustomDomain) != 0)
            {
                var length = (int)(bytes.Length - br.BaseStream.Position);
                result.CustomDomain = BlueskyRelationships.DecompressBpe(br.ReadBytes(length));
            }
            if ((format & DidDocEncoding.HasBskySocialUserName) != 0)
            {
                var length = (int)(bytes.Length - br.BaseStream.Position);
                result.BskySocialUserName = BlueskyRelationships.DecompressBpe(br.ReadBytes(length));
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

            Proto = HasBskySocialUserName | DidDocEncoding.HasCustomDomain,
        }
    }
}

