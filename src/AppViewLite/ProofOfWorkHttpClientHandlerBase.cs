using AppViewLite;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace AppViewLite
{
    public abstract class ProofOfWorkHttpClientHandlerBase : HttpMessageHandler
    {
        protected internal abstract void AddCookieUntyped(HttpRequestMessage request, object cookie);
        protected internal abstract Task<CookieWithExpiration<object>> PerformChallengeUntypedAsync(Uri baseUrl, object challenge, Action<HttpRequestMessage> setupRequest);
    }
    public abstract class ProofOfWorkHttpClientHandlerBase<TChallenge, TCookie> : ProofOfWorkHttpClientHandlerBase
    {
        private readonly HttpClient inner;
        protected HttpClient InnerHttpClient => inner;

        private readonly Dictionary<string, DataForDomain> cache = new();

        public ProofOfWorkHttpClientHandlerBase(HttpMessageHandler inner)
        {
            this.inner = new HttpClient(inner);
        }

        protected virtual string GetVaryKey(HttpRequestMessage request)
        {
            return request.RequestUri!.Host;
        }

        private ref DataForDomain GetOrCreateData(HttpRequestMessage request) => ref CollectionsMarshal.GetValueRefOrAddDefault(cache, GetVaryKey(request), out _);

        [MethodImpl(MethodImplOptions.Synchronized)]
        private Task<CookieWithExpiration<TCookie>> PerformChallengeOrReuseCookieAsync(HttpRequestMessage request, TChallenge challenge, Task<CookieWithExpiration<TCookie>>? knownBrokenCookie)
        {
            ref var data = ref GetOrCreateData(request);

            if (data.GetCookie == knownBrokenCookie)
                data.GetCookie = null;



            if (data.GetCookie == null || (data.GetCookie.Status == TaskStatus.RanToCompletion && DateTime.UtcNow > data.GetCookie.Result.Expiration))
            {
                if (data.LastChallengeResolutionAttempt != null && data.LastChallengeResolutionAttempt.Elapsed.TotalSeconds < 60)
                {
                    throw new Exception("Cookie proof of work: a challenge resolution was already recently attempted. Refusing to perform a new one.");
                }
                data.LastChallengeResolutionAttempt = Stopwatch.StartNew();

                var userAgent = request.Headers.UserAgent.ToString();
                data.GetCookie = PerformChallengeAsync(request.RequestUri!, challenge, req => 
                {
                    req.Headers.UserAgent.Clear();
                    if (!string.IsNullOrEmpty(userAgent))
                        req.Headers.UserAgent.ParseAdd(userAgent);
                });
            }
            return data.GetCookie;
        }

        protected sealed internal override void AddCookieUntyped(HttpRequestMessage request, object cookie)
        {
            AddCookie(request, (TCookie)cookie);
        }

        protected sealed internal override async Task<CookieWithExpiration<object>> PerformChallengeUntypedAsync(Uri baseUrl, object challenge, Action<HttpRequestMessage> setupRequest)
        {
            var result = await PerformChallengeAsync(baseUrl, (TChallenge)challenge, setupRequest);
            return new CookieWithExpiration<object>(result.Solution!, result.Expiration);
        }

        protected internal abstract void AddCookie(HttpRequestMessage request, TCookie cookie);

        protected abstract Task<CookieWithExpiration<TCookie>> PerformChallengeAsync(Uri baseUrl, TChallenge challenge, Action<HttpRequestMessage> setupRequest);

        public abstract Task<GenericNullable<TChallenge>> TryGetChallengeAsync(HttpResponseMessage response, CancellationToken ct);

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var cloner = await CreateRequestClonerAsync(request);
            var request1 = cloner();

            var data = GetOrCreateData(request);
            var preexistingCookie = data.GetCookie;

            if (preexistingCookie?.Status == TaskStatus.RanToCompletion)
            {
                AddCookie(request1, preexistingCookie.Result.Solution);
            }

            var response1 = await inner.SendAsync(request1, cancellationToken);

            var challenge = await TryGetChallengeAsync(response1, cancellationToken);
            if (challenge.IsNull) return response1;


            response1.Dispose();

            var result = await PerformChallengeOrReuseCookieAsync(request, challenge.Value, preexistingCookie);

            var request2 = cloner();
            AddCookie(request2, result.Solution);

            var response2 = await inner.SendAsync(request2, cancellationToken);

            return response2;
        }

        public static async Task<Func<HttpRequestMessage>> CreateRequestClonerAsync(HttpRequestMessage req)
        {
            var method = req.Method;
            var url = req.RequestUri;
            var body = req.Content != null ? await req.Content.ReadAsByteArrayAsync() : null;
            var contentHeaders = req.Content?.Headers;
            var headers = req.Headers;
            req.Dispose();

            return () =>
            {
                var clone = new HttpRequestMessage(method, url);

                if (body != null)
                {
                    clone.Content = new ByteArrayContent(body);

                    foreach (var h in contentHeaders!)
                        clone.Content.Headers.TryAddWithoutValidation(h.Key, h.Value);
                }

                foreach (var h in headers)
                    clone.Headers.TryAddWithoutValidation(h.Key, h.Value);
                
                return clone;
            };

        }

        internal record struct DataForDomain(Stopwatch? LastChallengeResolutionAttempt, Task<CookieWithExpiration<TCookie>>? GetCookie);
    }
    public record struct CookieWithExpiration<TCookie>(TCookie Solution, DateTime Expiration);

}

