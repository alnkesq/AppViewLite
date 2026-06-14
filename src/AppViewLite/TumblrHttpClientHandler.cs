using System;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AppViewLite
{
    class TumblrHttpClientHandler : HttpMessageHandler
    {
        private readonly HttpClient inner;

        private Stopwatch? lastChallengeResolutionAttempt;
        public TumblrHttpClientHandler(HttpMessageHandler inner)
        {
            this.inner = new HttpClient(inner);
        }


        private static async Task<Func<HttpRequestMessage>> CloneRequestMessageAsync(HttpRequestMessage req)
        {
            var method = req.Method;
            var url = req.RequestUri;
            var body = req.Content != null ? await req.Content.ReadAsByteArrayAsync() : null;
            var contentHeaders = req.Content?.Headers;
            var headers = req.Headers;

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


        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var cloner = await CloneRequestMessageAsync(request);
            var request1 = cloner();

            var preexistingHcp = getHcpAsync;
            
            if (preexistingHcp?.Status == TaskStatus.RanToCompletion)
            {
                AddHcpCookie(request1, preexistingHcp.Result);
            }

            var response1 = await inner.SendAsync(request1, cancellationToken);
            if (response1.StatusCode != System.Net.HttpStatusCode.Forbidden) return response1;

            var hccCookie = response1.GetSetCookie("_hcc");
            if (hccCookie == null) return response1;


            response1.Dispose();

            var result = await GetHcpAsync(request.RequestUri!, hccCookie, preexistingHcp);

            var request2 = cloner();
            AddHcpCookie(request2, result);

            var response2 = await inner.SendAsync(request2, cancellationToken);

            return response2;


        }

        private static void AddHcpCookie(HttpRequestMessage request, HcpResult hcp)
        {
            request.Headers.Add("Cookie", "_hcp=" + hcp.Hcp);
        }

        private Task<HcpResult>? getHcpAsync;

        [MethodImpl(MethodImplOptions.Synchronized)]
        private Task<HcpResult> GetHcpAsync(Uri baseUrl, string hccCookie, Task<HcpResult>? knownBrokenHcp)
        {
            if (getHcpAsync == knownBrokenHcp)
                getHcpAsync = null;

            if (getHcpAsync == null || (getHcpAsync.Status == TaskStatus.RanToCompletion && DateTime.UtcNow > getHcpAsync.Result.Expiration))
            {
                getHcpAsync = GetHcpCoreAsync(baseUrl, hccCookie);
            }
            return getHcpAsync;
        }


        private async Task<HcpResult> GetHcpCoreAsync(Uri baseUrl, string hccCookie)
        {

            if (lastChallengeResolutionAttempt != null && lastChallengeResolutionAttempt.Elapsed.TotalSeconds < 60)
            {
                throw new Exception("Tumblr: a challenge resolution was already recently attempted. Refusing to perform a new one.");
            }
            lastChallengeResolutionAttempt = Stopwatch.StartNew();

            var solution = GetSolution(hccCookie);


            var challengeRequest = new HttpRequestMessage(System.Net.Http.HttpMethod.Post, new Uri(baseUrl, "/__challenge"));
            challengeRequest.Headers.Add("X-Hashcash-Solution", solution.XHashcashSolution);
            challengeRequest.Headers.Add("X-Interactive", solution.XInteractive);
            challengeRequest.Headers.TryAddWithoutValidation("User-Agent", BlueskyEnrichedApis.DefaultUserAgent);

            await Task.Delay(3500);
            using var challengeResponse = await inner.SendAsync(challengeRequest);
            if (!challengeResponse.IsSuccessStatusCode)
            {
                Console.Error.WriteLine(await challengeResponse.Content.ReadAsStringAsync());
                throw new Exception("Tumblr: challenge response failed with HTTP " + (int)challengeResponse.StatusCode);
            }

            var hcp = challengeResponse.GetSetCookie("_hcp");
            if (hcp == null) throw new Exception("Tumblr: the server didn't return an _hcp cookie.");

            return new(hcp, DateTime.UtcNow.AddSeconds(3600));
        }

        private static readonly Encoding Latin1 = Encoding.Latin1;
        private static ParsedHccCookie ParseHccCookie(string i)
        {
            var e = Atob(i.Split(":")[1]);
            var r = e.Split("|");
            var t = int.Parse(r.ElementAtOrDefault(3) ?? "0");
            var n = r.ElementAtOrDefault(4) ?? "";
            var o = t >= 2;


            return new ParsedHccCookie(e, o, n, t);
        }

        private record struct ParsedHccCookie(
            string e,
            bool o, // needs checkbox
            string n, // X-Interactive (if checkbox needed)
            int t);
        private record struct HcpResult(string Hcp, DateTime Expiration);
        private static string Btoa(string input)
        {
            if (input.Any(c => c > 255))
                throw new ArgumentException("String contains characters outside Latin-1 range.");

            byte[] bytes = Latin1.GetBytes(input);
            return Convert.ToBase64String(bytes);
        }

        private static string Atob(string input)
        {
            byte[] bytes = Convert.FromBase64String(input);
            return Latin1.GetString(bytes);
        }

        
        private static (string XHashcashSolution, string XInteractive) GetSolution(string hcc)
        {
            try
            {
                var parsedHcc = ParseHccCookie(hcc);
                var hashcashSolutionBinary = ProofOfWork(parsedHcc.e, "0000");
                var xHashcashSolution = Btoa(hashcashSolutionBinary);

                var interactiveCheckboxSolution = parsedHcc.n;

                return (xHashcashSolution, parsedHcc.o ? interactiveCheckboxSolution : string.Empty);
            }
            catch (Exception ex)
            {
                throw new Exception("Tumblr: could not solve proof-of-work challenge: " + ex.Message);
            }


        }
        private static string ProofOfWork(string e, string t)
        {
            for (var n = 0; n < 2e8; n++)
            {
                var i = e + n;
                if (Sha256Hex(i).Substring(0, t.Length) == t) return i;
            }
            throw new Exception("Tumblr: Proof of work solution not found.");
        }
        private static string Sha256Hex(string e)
        {
            var t = Encoding.UTF8.GetBytes(e);
            var n = SHA256.HashData(t);
            return Convert.ToHexStringLower(n);
        }


    }
}

