using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AppViewLite
{
    public class BlocklistableHttpClientHandler : HttpMessageHandler
    {
        private readonly HttpMessageHandler inner;
        private MethodInfo invokeInnerMethod;
        private bool disposeInner;
        public BlocklistableHttpClientHandler(HttpMessageHandler inner, bool disposeInner)
        {
            this.inner = inner;
            this.disposeInner = disposeInner;
            this.invokeInnerMethod = inner.GetType().GetMethod("SendAsync", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance, [typeof(HttpRequestMessage), typeof(CancellationToken)])!;
        }


        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            AdministrativeBlocklist.Instance.GetValue().ThrowIfBlockedOutboundConnection(request.RequestUri!.Host);
            using var _ = await HostRateLimiter.AcquireUrlAsync(request.RequestUri, cancellationToken);
            return await (Task<HttpResponseMessage>)invokeInnerMethod.Invoke(inner, [request, cancellationToken])!;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposeInner)
                inner.Dispose();
        }
    }
}

