using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using AppViewLite;

namespace AppViewLite
{
    class TumblrHttpClientHandler : ProofOfWorkHttpClientHandlerBase<string, string>
    {

        public TumblrHttpClientHandler(HttpMessageHandler inner)
            : base(inner)
        {
        }


        protected override void AddCookie(HttpRequestMessage request, string cookie)
        {
            request.Headers.Add("Cookie", "_hcp=" + cookie);
        }

        public override bool TryGetChallenge(HttpResponseMessage response, [NotNullWhen(true)] out string? challenge)
        {
            challenge = null;
            if (response.StatusCode != System.Net.HttpStatusCode.Forbidden) return false;

            challenge = response.GetSetCookie("_hcc");
            return challenge != null;

        }

        protected async override Task<CookieWithExpiration> PerformChallengeAsync(Uri baseUrl, string challenge)
        {

            var solution = GetSolution(challenge);

            var challengeRequest = new HttpRequestMessage(System.Net.Http.HttpMethod.Post, new Uri(baseUrl, "/__challenge"));
            challengeRequest.Headers.Add("X-Hashc" + "ash-Solution", solution.XHashcashSolution);
            challengeRequest.Headers.Add("X-Inte" + "ractive", solution.XInteractive);
            challengeRequest.Headers.TryAddWithoutValidation("User-Agent", BlueskyEnrichedApis.DefaultUserAgent);

            await Task.Delay(3500);
            if (!string.IsNullOrEmpty(solution.XInteractive))
                await Task.Delay(1200);
            using var challengeResponse = await InnerHttpClient.SendAsync(challengeRequest);
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

