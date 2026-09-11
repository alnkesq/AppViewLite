using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace AppViewLite
{
    public class AccessibleHttpMessageHandler : DelegatingHandler
    {
        public AccessibleHttpMessageHandler(HttpMessageHandler inner)
            : base(inner)
        { 
        }

        protected override HttpResponseMessage Send(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            return base.Send(request, cancellationToken);
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            return base.SendAsync(request, cancellationToken);
        }



        public HttpResponseMessage PublicSend(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            return this.Send(request, cancellationToken);
        }

        public Task<HttpResponseMessage> PublicSendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            return this.SendAsync(request, cancellationToken);
        }



    }
}

