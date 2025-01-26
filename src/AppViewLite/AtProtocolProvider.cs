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
        public IReadOnlyList<AlternatePds> HostsAndJetstreams { get; private set; }
        public string DefaultHost { get; private set; }
        public IReadOnlyList<AlternatePds> AtProtoHosts { get; private set; }

        public AtProtocolProvider(IReadOnlyList<AlternatePds> hosts)
        {
            this.HostsAndJetstreams = hosts;
            this.AtProtoHosts = hosts.Where(x => !x.IsJetStream).ToArray();
            this.DefaultHost = AtProtoHosts.FirstOrDefault(x => x.IsWildcard).Host ?? "https://bsky.network";
        }

        public string GetHostOrRelayForDid(string did) => TryGetPdsForDid(did) ?? AtProtoHosts.FirstOrDefault(x => x.IsWildcard).Host;
        public string TryGetPdsForDid(string did) => AtProtoHosts.FirstOrDefault(x => x.IsDid && x.Did == did).Host;

        public ATProtocol CreateProtocolForDid(string did)
        {
            return CreateProtocolForDidCore(did, defaultToRelay: true);
        }
        public ATProtocol CreateProtocolForDidForLogin(string did)
        {
            return CreateProtocolForDidCore(did, defaultToRelay: false);
        }
        private ATProtocol CreateProtocolForDidCore(string did, bool defaultToRelay)
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
                if (defaultToRelay)
                    builder.WithInstanceUrl(new Uri(AtProtoHosts.FirstOrDefault(x => x.IsWildcard).Host));
            }
            return builder.Build();
        }

        public static AtProtocolProvider CreateDefault()
        {
            return new AtProtocolProvider([new("*", "https://bsky.network")]);
        }
    }
}

