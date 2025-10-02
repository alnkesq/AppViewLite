using System;
using System.Linq;
using System.Collections.Generic;

namespace AppViewLite
{
    public class FirehoseUrlWithFallbacks
    {
        public readonly IReadOnlyList<Uri> Urls;
        public readonly bool IsJetStream;
        public readonly string CanonicalIdentifier;
        private int _index;

        public FirehoseUrlWithFallbacks(IReadOnlyList<Uri> urls, bool isJetStream = false, string? canonicalIdentifier = null)
        {
            this.Urls = urls;
            this.IsJetStream = isJetStream;
            this.CanonicalIdentifier = canonicalIdentifier ?? urls[0].AbsoluteUri;
        }

        public override string ToString() => CanonicalIdentifier;
        public Uri GetNext()
        {
            var url = Urls[_index];
            if (_index == Urls.Count - 1) _index = 0;
            else _index++;
            return url;
        }

        public static FirehoseUrlWithFallbacks Parse(string urls)
        {
            string? canonicalIdentifier = null;
            if (urls.StartsWith('['))
            { 
                var closeSquare = urls.IndexOf(']');
                canonicalIdentifier = urls.Substring(1, closeSquare - 1);
                urls = urls.Substring(closeSquare + 1);
            }
            bool? isJetStream = null;
            var parsedUrls = urls.Split('|', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Select(x =>
            {
                var currentIsJet = false;
                if (x.StartsWith("jet:", StringComparison.Ordinal))
                {
                    currentIsJet = true;
                    x = x.Substring(4);
                }
                if (isJetStream != null)
                {
                    if (isJetStream != currentIsJet) throw new ArgumentException("The fallback URLs of a firehose must all be of the same kind (JetStream or ATProto)");
                }
                isJetStream = currentIsJet;
                return new Uri("https://" + x);
            }).ToArray();
            if (parsedUrls.Length == 0) throw new ArgumentException("Empty firehose URL.");
            return new FirehoseUrlWithFallbacks(parsedUrls, isJetStream: isJetStream!.Value, canonicalIdentifier: canonicalIdentifier);
        }


    }
}

