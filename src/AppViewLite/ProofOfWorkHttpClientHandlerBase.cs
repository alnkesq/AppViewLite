using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace AppViewLite
{
    public abstract class ProofOfWorkHttpClientHandlerBase<TChallenge, TCookie> : HttpMessageHandler
    {
        private readonly HttpClient inner;
        protected HttpClient InnerHttpClient => inner;

        private Stopwatch? lastChallengeResolutionAttempt;
        public ProofOfWorkHttpClientHandlerBase(HttpMessageHandler inner)
        {
            this.inner = new HttpClient(inner);
        }

        public record struct CookieWithExpiration(TCookie Solution, DateTime Expiration);

        private Task<CookieWithExpiration>? getCookie;

        [MethodImpl(MethodImplOptions.Synchronized)]
        private Task<CookieWithExpiration> PerformChallengeOrReuseCookieAsync(Uri baseUrl, TChallenge challenge, Task<CookieWithExpiration>? knownBrokenCookie)
        {
            if (getCookie == knownBrokenCookie)
                getCookie = null;

            if (getCookie == null || (getCookie.Status == TaskStatus.RanToCompletion && DateTime.UtcNow > getCookie.Result.Expiration))
            {
                if (lastChallengeResolutionAttempt != null && lastChallengeResolutionAttempt.Elapsed.TotalSeconds < 60)
                {
                    throw new Exception("Cookie proof of work: a challenge resolution was already recently attempted. Refusing to perform a new one.");
                }
                lastChallengeResolutionAttempt = Stopwatch.StartNew();
                getCookie = PerformChallengeAsync(baseUrl, challenge);
            }
            return getCookie;
        }

        protected abstract void AddCookie(HttpRequestMessage request, TCookie cookie);

        protected abstract Task<CookieWithExpiration> PerformChallengeAsync(Uri baseUrl, TChallenge challenge);

        public abstract bool TryGetChallenge(HttpResponseMessage response, [NotNullWhen(true)] out TChallenge? challenge);

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var cloner = await CreateRequestClonerAsync(request);
            var request1 = cloner();

            var preexistingCookie = getCookie;

            if (preexistingCookie?.Status == TaskStatus.RanToCompletion)
            {
                AddCookie(request1, preexistingCookie.Result.Solution);
            }

            var response1 = await inner.SendAsync(request1, cancellationToken);

            if (!TryGetChallenge(response1, out var challenge)) return response1;
            

            response1.Dispose();

            var result = await PerformChallengeOrReuseCookieAsync(request.RequestUri!, challenge, preexistingCookie);

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


    }
}

