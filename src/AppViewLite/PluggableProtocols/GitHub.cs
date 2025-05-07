using AppViewLite.Models;
using AppViewLite.PluggableProtocols.Rss;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace AppViewLite.PluggableProtocols
{
    public static class GitHub
    {
        public static async Task<VirtualRssResult> GetCommitsAsync(string did, string owner, string repo)
        {
            var response = (await BlueskyEnrichedApis.DefaultHttpClientForRss.GetFromJsonAsync<GitHubCommitEntry[]>($"https://api.github.com/repos/{owner}/{repo}/commits"))!;
            return new VirtualRssResult(new Models.BlueskyProfileBasicInfo { DisplayName = repo + " (commits)", CustomFields = [new CustomFieldProto("web", $"https://github.com/{owner}/{repo}/commits")] },
                response
                .Where(x => !IsNoiseAuthor(x.author))
                .Select(x =>
            {
                var url = $"https://github.com/{owner}/{repo}/commit/{x.sha}";
                var id = new QualifiedPluggablePostId(did, new NonQualifiedPluggablePostId(PluggableProtocol.CreateSyntheticTid(x.commit.author.date, x.sha), url));
                var parts = x.commit.message.Split('\n', 2);
                var text = string.Join("\n", parts.ElementAtOrDefault(1)?.Split("\n", StringSplitOptions.TrimEntries)
                    .Where(x => x != "---------" && !x.StartsWith("Co-authored-by:", StringComparison.Ordinal))
                    .Select(x => AsteriskToBullet(x)) ?? []).Trim();
                return new VirtualRssPost(id, new BlueskyPostData { Text = StringUtils.TrimTextWithEllipsis(Regex.Replace(text, "\n{2,}• ", "\n• "), 1000, 14), ExternalTitle = parts[0].Trim(), ExternalUrl = url });
            }).ToArray());
        }

        private static bool IsNoiseAuthor(GitHubUserExtended? user)
        {

            if (user == null) return false;
            return
                user.id is
                    49699333 // dependabot[bot]
                    ;
        }

        private static string AsteriskToBullet(string line)
        {
            return line.StartsWith("* ", StringComparison.Ordinal) ? string.Concat("• ", line.AsSpan(2)) : line;
        }

        public static async Task<VirtualRssResult> GetReleasesAsync(string did, string owner, string repo)
        {
            var response = (await BlueskyEnrichedApis.DefaultHttpClientForRss.GetFromJsonAsync<GitHubRelease[]>($"https://api.github.com/repos/{owner}/{repo}/releases"))!;
            return new VirtualRssResult(new Models.BlueskyProfileBasicInfo { DisplayName = repo + " (releases)", CustomFields = [new CustomFieldProto("web", $"https://github.com/{owner}/{repo}/releases")] }, response.Select(x =>
            {
                var url = $"https://github.com/{owner}/{repo}/releases/tag/{x.tag_name}";
                var id = new QualifiedPluggablePostId(did, new NonQualifiedPluggablePostId(PluggableProtocol.CreateSyntheticTid(x.published_at, x.tag_name), url));
                return new VirtualRssPost(id, new BlueskyPostData { ExternalTitle = x.name, PluggableLikeCount = x.reactions?.total_count, ExternalUrl = url, ExternalDescription = StringUtils.TrimTextWithEllipsis(string.Join("\n", (x.body?.Split('\n') ?? []).Select(AsteriskToBullet)), 1000, 14) });
            }).ToArray());
        }

        public static async Task<VirtualRssResult> GetIssuesAsync(string did, string owner, string repo, bool pulls)
        {
            var response = (await BlueskyEnrichedApis.DefaultHttpClientForRss.GetFromJsonAsync<GitHubIssue[]>($"https://api.github.com/repos/{owner}/{repo}/issues?state=all&filter=all&per_page=100"))!;
            return new VirtualRssResult(new Models.BlueskyProfileBasicInfo { DisplayName = repo + (pulls ? " (pull requests)" : " (issues)"), CustomFields = [new CustomFieldProto("web", $"https://github.com/{owner}/{repo}/issues")] }, response
                .Where(x => (x.pull_request != null) == pulls)
                .Where(x => !IsNoiseAuthor(x.user))
                .Select(x =>
            {
                var url = $"https://github.com/{owner}/{repo}/{(x.pull_request != null ? "pull" : "issues")}/{x.number}";
                var id = new QualifiedPluggablePostId(did, new NonQualifiedPluggablePostId(PluggableProtocol.CreateSyntheticTid(x.created_at, x.number.ToString()), url));
                return new VirtualRssPost(id, new BlueskyPostData { ExternalTitle = x.title, PluggableLikeCount = x.reactions?.total_count, PluggableReplyCount = x.comments, ExternalUrl = url, ExternalDescription = StringUtils.TrimTextWithEllipsis(x.body, 1000, pulls ? 14 : 10) });
            }).ToArray());
        }




#nullable disable


        public class GitHubCommitEntry
        {
            public string sha { get; set; }
            public string node_id { get; set; }
            public GitHubCommit commit { get; set; }
            public string url { get; set; }
            public string html_url { get; set; }
            public string comments_url { get; set; }
            public GitHubUserExtended author { get; set; }
            public GitHubUserExtended committer { get; set; }
            public GitHubGitParent[] parents { get; set; }
        }

        public class GitHubCommit
        {
            public GitHubGitUser author { get; set; }
            public GitHubGitUser committer { get; set; }
            public string message { get; set; }
            public GitHubTree tree { get; set; }
            public string url { get; set; }
            public int comment_count { get; set; }
            public GitHubVerification verification { get; set; }
        }

        public class GitHubGitUser
        {
            public string name { get; set; }
            public string email { get; set; }
            public DateTime date { get; set; }
        }

        public class GitHubTree
        {
            public string sha { get; set; }
            public string url { get; set; }
        }

        public class GitHubVerification
        {
            public bool verified { get; set; }
            public string reason { get; set; }
            public string signature { get; set; }
            public string payload { get; set; }
            public DateTime? verified_at { get; set; }
        }

        public class GitHubUserExtended
        {
            public string login { get; set; }
            public int id { get; set; }
            public string node_id { get; set; }
            public string avatar_url { get; set; }
            public string gravatar_id { get; set; }
            public string url { get; set; }
            public string html_url { get; set; }
            public string followers_url { get; set; }
            public string following_url { get; set; }
            public string gists_url { get; set; }
            public string starred_url { get; set; }
            public string subscriptions_url { get; set; }
            public string organizations_url { get; set; }
            public string repos_url { get; set; }
            public string events_url { get; set; }
            public string received_events_url { get; set; }
            public string type { get; set; }
            public string user_view_type { get; set; }
            public bool site_admin { get; set; }
        }

        public class GitHubGitParent
        {
            public string sha { get; set; }
            public string url { get; set; }
            public string html_url { get; set; }
        }







        public class GitHubRelease
        {
            public string url { get; set; }
            public string assets_url { get; set; }
            public string upload_url { get; set; }
            public string html_url { get; set; }
            public int id { get; set; }
            public GitHubUserExtended author { get; set; }
            public string node_id { get; set; }
            public string tag_name { get; set; }
            public string target_commitish { get; set; }
            public string name { get; set; }
            public bool draft { get; set; }
            public bool prerelease { get; set; }
            public DateTime created_at { get; set; }
            public DateTime published_at { get; set; }
            public GitHubReleaseAsset[] assets { get; set; }
            public string tarball_url { get; set; }
            public string zipball_url { get; set; }
            public string body { get; set; }
            public GitHubReactions reactions { get; set; }
            public int mentions_count { get; set; }
        }

        public class GitHubReactions
        {
            public string url { get; set; }
            public int total_count { get; set; }
            [JsonPropertyName("+1")] public int Plus1 { get; set; }
            [JsonPropertyName("-1")] public int Minus1 { get; set; }
            public int laugh { get; set; }
            public int hooray { get; set; }
            public int confused { get; set; }
            public int heart { get; set; }
            public int rocket { get; set; }
            public int eyes { get; set; }
        }

        public class GitHubReleaseAsset
        {
            public string url { get; set; }
            public int id { get; set; }
            public string node_id { get; set; }
            public string name { get; set; }
            public string label { get; set; }
            public GitHubUserExtended uploader { get; set; }
            public string content_type { get; set; }
            public string state { get; set; }
            public int size { get; set; }
            public int download_count { get; set; }
            public DateTime created_at { get; set; }
            public DateTime updated_at { get; set; }
            public string browser_download_url { get; set; }
        }






        public class GitHubIssue
        {
            public string url { get; set; }
            public string repository_url { get; set; }
            public string labels_url { get; set; }
            public string comments_url { get; set; }
            public string events_url { get; set; }
            public string html_url { get; set; }
            public long id { get; set; }
            public string node_id { get; set; }
            public int number { get; set; }
            public string title { get; set; }
            public GitHubUserExtended user { get; set; }
            public GitHubLabel[] labels { get; set; }
            public string state { get; set; }
            public bool locked { get; set; }
            public GitHubUserExtended assignee { get; set; }
            public GitHubUserExtended[] assignees { get; set; }
            public GitHubMilestone milestone { get; set; }
            public int comments { get; set; }
            public DateTime created_at { get; set; }
            public DateTime updated_at { get; set; }
            public object closed_at { get; set; }
            public string author_association { get; set; }
            public object type { get; set; }
            public object active_lock_reason { get; set; }
            public bool draft { get; set; }
            public GitHubPullRequest pull_request { get; set; }
            public string body { get; set; }
            public object closed_by { get; set; }
            public GitHubReactions reactions { get; set; }
            public string timeline_url { get; set; }
            public object performed_via_github_app { get; set; }
            public object state_reason { get; set; }
            public GitHubSubIssuesSummary sub_issues_summary { get; set; }
        }



        public class GitHubMilestone
        {
            public string url { get; set; }
            public string html_url { get; set; }
            public string labels_url { get; set; }
            public int id { get; set; }
            public string node_id { get; set; }
            public int number { get; set; }
            public string title { get; set; }
            public string description { get; set; }
            public GitHubUserExtended creator { get; set; }
            public int open_issues { get; set; }
            public int closed_issues { get; set; }
            public string state { get; set; }
            public DateTime created_at { get; set; }
            public DateTime updated_at { get; set; }
            public object due_on { get; set; }
            public object closed_at { get; set; }
        }

        public class GitHubPullRequest
        {
            public string url { get; set; }
            public string html_url { get; set; }
            public string diff_url { get; set; }
            public string patch_url { get; set; }
            public object merged_at { get; set; }
        }



        public class GitHubSubIssuesSummary
        {
            public int total { get; set; }
            public int completed { get; set; }
            public int percent_completed { get; set; }
        }

        public class GitHubLabel
        {
            public long id { get; set; }
            public string node_id { get; set; }
            public string url { get; set; }
            public string name { get; set; }
            public string color { get; set; }
            public bool _default { get; set; }
            public string description { get; set; }
        }


    }


}

