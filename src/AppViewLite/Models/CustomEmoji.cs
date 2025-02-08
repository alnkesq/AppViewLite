using ProtoBuf;
using DuckDbSharp.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    [ProtoContract]
    public class CustomEmoji
    {
        [ProtoMember(1)] public required string ShortCode;
        [ProtoMember(2)] public required string Url;

        public DuckDbUuid Hash => StringUtils.HashUnicodeToUuid(ShortCode.Length + " " + ShortCode + Url);

        public override string ToString()
        {
            return ":" + ShortCode + ":";
        }
    }
}

