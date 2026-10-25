using CarpaNet;
using ComAtproto.Repo;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;

namespace AppViewLite
{
    public static class CarpaNetCompat
    {
        extension(JsonElement json)
        { 
            public string AtProtoTypeId
            {
                get
                {
                    if (json.TryGetProperty("$type", out var value) && value.GetString() is { } result)
                    {
                        return result;
                    }
                    throw new NotSupportedException("Unknown $type for provided JSON object.");
                }
            }
        }

        extension(ATUri uri)
        {
            public string DidAsString
            {
                get
                {
                    if (uri.Value.StartsWith("did:", StringComparison.Ordinal)) return uri.Value;
                    return uri.AuthorityAsDid!.Value.DidAsString;
                }
            }

            public string? Rkey => uri.RecordKey;
        }
        extension(ATCid cid)
        {
            public byte[] ToBytesV2()
            {
                if (!cid.IsValid) return null;
                return cid.ToBytes();
            }
        }
        extension(ATDid did)
        {
            public string DidAsString
            {
                get
                {
                    if (string.IsNullOrEmpty(did.Value)) throw new InvalidOperationException("Empty did.");
                    if (!did.Value.StartsWith("did:", StringComparison.Ordinal))
                        throw new InvalidOperationException("Not a did:.");
                    return did.Value;
                }
            }
        }
        extension(StrongRef)
        {
            public static StrongRef Create(ATUri uri, string cid) => new StrongRef {  Uri = uri, Cid = cid  };
        }
    }
}

