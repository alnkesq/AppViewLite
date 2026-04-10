using DuckDbSharp.Types;
using ProtoBuf;
using System;
using System.Diagnostics.CodeAnalysis;

namespace AppViewLite.Models
{
    [ProtoContract]
    public class CustomEmoji
    {
        private CustomEmoji()
        {
        }

        [SetsRequiredMembers]
        public CustomEmoji(string shortCode, string url)
        {
            if (shortCode.Contains(':')) throw new ArgumentException("CustomEmoji shortCode should not include colons.");
            this.ShortCode = shortCode;
            this.Url = url;
        }
        [ProtoMember(1)] public required string ShortCode;
        [ProtoMember(2)] public required string Url;

        public DuckDbUuid Hash => StringUtils.HashUnicodeToUuid(ShortCode.Length + " " + ShortCode + Url);

        public override string ToString()
        {
            return ":" + ShortCode + ":";
        }
    }
}

