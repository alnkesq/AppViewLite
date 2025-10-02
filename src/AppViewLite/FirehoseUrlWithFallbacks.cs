using System;
using System.Linq;
using System.Collections.Generic;

namespace AppViewLite
{
    public class FirehoseUrlWithFallbacks(IReadOnlyList<Uri> urls, bool isJetStream = false)
    {
        public bool IsJetStream => isJetStream;
        public IReadOnlyList<Uri> Urls => urls;
        private int _index;

        public override string ToString() => CanonicalIdentifier;
        public Uri GetNext()
        {
            var url = urls[_index];
            if (_index == urls.Count - 1) _index = 0;
            else _index++;
            return url;
        }

        public static FirehoseUrlWithFallbacks Parse(string urls)
        {
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
            return new FirehoseUrlWithFallbacks(parsedUrls, isJetStream: isJetStream!.Value);
        }

        public string CanonicalIdentifier => urls[0].AbsoluteUri;


    }
}

