using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public record struct AlternatePds(string DidOrWildcard, string Host, bool IsJetStream = false, bool ListenFirehose = true)
    {
        public bool IsWildcard => DidOrWildcard == "*";
        public bool IsDid => !IsWildcard;
        public string Did => IsDid ? DidOrWildcard : throw new InvalidOperationException();
    }
}

