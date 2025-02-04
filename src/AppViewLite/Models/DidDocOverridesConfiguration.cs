using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public class DidDocOverridesConfiguration
    {
        public Dictionary<string, (string Pds, string[] Handles)> CustomDidDocs = new();
        private DateTime date;

        internal static DidDocOverridesConfiguration ReadFromFile(string path)
        {
            var result = new DidDocOverridesConfiguration();
            result.date = File.GetLastWriteTimeUtc(path);

            foreach (var line_ in File.ReadLines(path))
            {
                var line = line_.Trim();
                var hash = line.IndexOf('#');
                if (hash != -1)
                    line = line.Substring(0, hash).Trim();

                var fields = line.Split(' ', '\t').Select(x => x.Trim()).Where(x => !string.IsNullOrEmpty(x)).ToArray();

                // Format: did pds handle1,handle2
                var did = fields[0];
                var pds = fields[1];
                var handles = fields.ElementAtOrDefault(2)?.Split(",");
                if (!pds.Contains('/')) pds = "https://" + pds;
                BlueskyEnrichedApis.EnsureValidDid(did);
                result.CustomDidDocs.Add(fields[0], (pds, handles ?? []));
            }
            return result;
        }

        internal DidDocProto? TryGetOverride(string did)
        {
            if (!CustomDidDocs.TryGetValue(did, out var overrideDoc)) return null;
            
            return new DidDocProto
            {
                CustomDomain = overrideDoc.Handles.Length == 1 ? overrideDoc.Handles[0] : null,
                MultipleHandles = overrideDoc.Handles.Length > 1 ? overrideDoc.Handles : null,
                Date = date,
                Pds = overrideDoc.Pds,

            };
        }
    }
}

