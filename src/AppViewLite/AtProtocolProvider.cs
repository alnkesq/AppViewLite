using FishyFlip;
using AppViewLite.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    public class AtProtocolProvider
    {
        public IReadOnlyList<AlternatePds> Hosts { get; private set; }
        public string DefaultHost { get; private set; }

        public AtProtocolProvider(IReadOnlyList<AlternatePds> hosts)
        {
            this.Hosts = hosts;
            this.DefaultHost = hosts.FirstOrDefault(x => x.IsWildcard).Host ?? "https://bsky.network";
        }

        public string GetHostOrRelayForDid(string did) => TryGetPdsForDid(did) ?? Hosts.FirstOrDefault(x => x.IsWildcard).Host;
        public string TryGetPdsForDid(string did) => Hosts.FirstOrDefault(x => x.IsDid && x.Did == did).Host;

        public ATProtocol CreateProtocolForDid(string did)
        {
            var pds = TryGetPdsForDid(did);
            var builder = new ATProtocolBuilder();
            if (pds != null)
            {
                builder.WithInstanceUrl(new Uri(pds));
                var dict = new Dictionary<FishyFlip.Models.ATDid, Uri>();
                dict.Add(new FishyFlip.Models.ATDid(did), new Uri(pds));
                builder.WithATDidCache(dict);
            }
            else
            {
                builder.WithInstanceUrl(new Uri(Hosts.FirstOrDefault(x => x.IsWildcard).Host));
            }
            return builder.Build();
        }

        public static AtProtocolProvider CreateDefault()
        {
            return new AtProtocolProvider([new("*", "https://bsky.network")]);
        }
    }
}

