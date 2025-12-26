using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Reflection;
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

        public TimeSpan? Timeout { get; set; }
        
        public string? RateLimitingRealm { get; set; }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            AdministrativeBlocklist.Instance.GetValue().ThrowIfBlockedOutboundConnection(request.RequestUri!.Host);
            using var _ = await HostRateLimiter.AcquireUrlAsync(request.RequestUri, RateLimitingRealm, cancellationToken);

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            if (Timeout != null)
            {
                cts.CancelAfter(Timeout.Value);
                cancellationToken = cts.Token;
            }
            return await (Task<HttpResponseMessage>)invokeInnerMethod.Invoke(inner, [request, cancellationToken])!;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposeInner)
                inner.Dispose();
        }


        public static async ValueTask<Stream> ConnectCallbackForbidLocalIps(SocketsHttpConnectionContext ctx, CancellationToken ct)
        {
            IPAddress[] addresses = await Dns.GetHostAddressesAsync(ctx.DnsEndPoint.Host, ct);

            if (addresses.Length == 0)
                throw new HttpRequestException("DNS returned no addresses for " + ctx.DnsEndPoint.Host);

            Uri target = ctx.InitialRequestMessage.RequestUri!;
            string connectHost = ctx.DnsEndPoint.Host;

            bool viaProxy = !string.Equals(connectHost, target.IdnHost, StringComparison.OrdinalIgnoreCase);
            if (viaProxy)
            {
                // We can't know for sure, since it's the proxy that will actually open the connection.
                // The proxy should be configured to refuse localhost IPs.
                // At least however provide some better exception user experience by checking some common local hosts/IPs.
                if (target.Host.Equals("localhost", StringComparison.OrdinalIgnoreCase) || (IPAddress.TryParse(target.Host, out var parsedIp) && IsLocalNetworkIp(parsedIp)))
                {
                    throw new HttpRequestException("Refusing to connect to an HTTP endpoint on the local network.");
                }

            }


            foreach (var (index, ip) in addresses.Index())
            {
                if (!viaProxy && IsLocalNetworkIp(ip))
                    throw new HttpRequestException("Refusing to connect to an HTTP endpoint on the local network.");

                var socket = new Socket(ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
                {
                    NoDelay = true
                };

                try
                {
                    using (ct.Register(() => socket.Dispose()))
                    {
                        await socket.ConnectAsync(
                            new IPEndPoint(ip, ctx.DnsEndPoint.Port),
                            ct);
                    }

                    return new NetworkStream(socket, ownsSocket: true);
                }
                catch (Exception) when (index != addresses.Length - 1) // retry with a different address unless this was the last one
                {
                    socket.Dispose();
                }
            }

            throw new UnreachableException();

        }

        private static bool IsLocalNetworkIp(IPAddress ip)
        {
            if (IPAddress.IsLoopback(ip))
                return true;

            if (ip.AddressFamily == AddressFamily.InterNetworkV6)
            {
                if (ip.IsIPv6LinkLocal ||
                    ip.IsIPv6SiteLocal ||
                    ip.IsIPv6UniqueLocal)
                    return true;
            }
            else if (ip.AddressFamily == AddressFamily.InterNetwork)
            {
                Span<byte> b = stackalloc byte[4];
                if (!ip.TryWriteBytes(b, out var spanLength) || spanLength != 4) throw new UnreachableException();

                if (b[0] == 10 ||
                    (b[0] == 172 && b[1] >= 16 && b[1] <= 31) ||
                    (b[0] == 192 && b[1] == 168) ||
                    (b[0] == 169 && b[1] == 254) ||
                    b[0] == 0)
                    return true;
            }
            else throw new NotSupportedException();

            return false;
        }
    }
}

