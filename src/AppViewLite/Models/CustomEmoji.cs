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
        public CustomEmoji()
        { 
        }
        public CustomEmoji(string shortCode, string url)
        {
            if (shortCode.Contains(':')) throw new ArgumentException();
            this.ShortCode = shortCode;
            this.Url = url;
        }
        [ProtoMember(1)] public string ShortCode;
        [ProtoMember(2)] public string Url;

        public DuckDbUuid Hash => StringUtils.HashUnicodeToUuid(ShortCode.Length + " " + ShortCode + Url);

        public override string ToString()
        {
            return ":" + ShortCode + ":";
        }
    }
}

