using CarpaNet;
using ComAtproto.Repo;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;

namespace AppViewLite
{
    internal static class CarpaNetCompat
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
            public string DidAsString => uri.AuthorityAsDid!.Value.DidAsString;
            public string? Rkey => uri.RecordKey;
        }
        extension(ATDid did)
        {
            public string DidAsString
            {
                get
                {
                    if (did.Value is null || !did.Value.StartsWith("did:", StringComparison.Ordinal))
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

