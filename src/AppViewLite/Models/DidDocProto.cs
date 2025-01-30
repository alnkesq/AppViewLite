using AppViewLite.Numerics;
using System;
using System.IO;
using System.Runtime.CompilerServices;

namespace AppViewLite.Models
{
    //[ProtoContract] --
    public class DidDocProto
    {
        /*[ProtoMember(1)] */ public string? BskySocialUserName;
        /*[ProtoMember(2)] */ public int? PdsId;
        /*[ProtoMember(3)] */ public string? CustomDomain;
        /*[ProtoMember(4)] */ public DateTime Date;
        /*[ProtoMember(5)] */ public string? Pds;

        public string Handle => (CustomDomain ?? (BskySocialUserName != null ? BskySocialUserName + ".bsky.social" : null))!;

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
            var date = Date;
            if (date < ApproximateDateTime32.MinValueAsDateTime)
                date = default;
            bw.Write(Unsafe.BitCast<ApproximateDateTime32, uint>((ApproximateDateTime32)date));
            if (PdsId != null)
            {
                format |= DidDocEncoding.HasPds;
                bw.Write7BitEncodedInt(PdsId.Value);
            }
            if (CustomDomain != null)
            {
                format |= DidDocEncoding.HasCustomDomain;
                lock (BlueskyRelationships.textCompressorUnlocked)
                {
                    bw.Write(BlueskyRelationships.textCompressorUnlocked.Compress(CustomDomain));
                }
            }
            else if (BskySocialUserName != null)
            {
                format |= DidDocEncoding.HasBskySocialUserName;
                lock (BlueskyRelationships.textCompressorUnlocked)
                {
                    bw.Write(BlueskyRelationships.textCompressorUnlocked.Compress(BskySocialUserName));
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
            var result = new DidDocProto();
            result.Date = Unsafe.BitCast<uint, ApproximateDateTime32>(br.ReadUInt32());
            if ((format & DidDocEncoding.HasPds) != 0)
            {
                result.PdsId = br.Read7BitEncodedInt();
            }
            if ((format & DidDocEncoding.HasCustomDomain) != 0)
            {
                var length = (int)(bytes.Length - br.BaseStream.Position);
                lock (BlueskyRelationships.textCompressorUnlocked)
                {
                    result.CustomDomain = BlueskyRelationships.textCompressorUnlocked.Decompress(br.ReadBytes(length));
                }
            }
            if ((format & DidDocEncoding.HasBskySocialUserName) != 0)
            {
                var length = (int)(bytes.Length - br.BaseStream.Position);
                lock (BlueskyRelationships.textCompressorUnlocked)
                {
                    result.BskySocialUserName = BlueskyRelationships.textCompressorUnlocked.Decompress(br.ReadBytes(length));
                }
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
        }
    }
}

