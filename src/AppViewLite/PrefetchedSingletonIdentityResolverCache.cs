using CarpaNet.Identity;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AppViewLite
{
    internal class PrefetchedSingletonIdentityResolverCache : IIdentityCache
    {
        internal required string Did;
        internal required string Pds;

        public void Clear()
        {
            throw new NotSupportedException();
        }

        public Task<DidDocument?> GetDidDocumentAsync(string did, CancellationToken cancellationToken = default)
        {
            if (this.Did != did) throw new NotSupportedException();
            return Task.FromResult<DidDocument?>(new DidDocument { Id = did, Service = [new DidService { Id = "#atproto_pds", Type = "AtprotoPersonalDataServer", ServiceEndpoint = Pds }] });
        }

        public Task<string?> GetHandleDidAsync(string handle, CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException();
        }

        public void RemoveDidDocument(string did)
        {
            throw new NotSupportedException();
        }

        public void RemoveHandle(string handle)
        {
            throw new NotSupportedException();
        }

        public Task SetDidDocumentAsync(string did, DidDocument document, CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException();
        }

        public Task SetHandleDidAsync(string handle, string did, CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException();
        }
    }
}

