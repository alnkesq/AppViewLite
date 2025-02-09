using AppViewLite.Models;
using AppViewLite.Numerics;
using FishyFlip.Lexicon.App.Bsky.Feed;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    public class MergeablePostEnumerator
    {
        public Tid LastReturnedTid;
        private Func<Tid, Task<PostReference[]>> RetrieveAsync;
        public MergeablePostEnumerator(Tid lastReturnedTid, Func<Tid, Task<PostReference[]>> retrieve, CollectionKind kind)
        {
            this.LastReturnedTid = lastReturnedTid;
            this.RetrieveAsync = retrieve;
            this.Kind = kind;
        }
        public CollectionKind Kind;

        public async Task<ParsedPostReference[]> GetNextPageAsync()
        {
            if (LastReturnedTid == default) return [];

            return (await RetrieveAsync(LastReturnedTid)).Select(x =>
            {
                if (!Tid.TryParse(x.RKey, out var rkey)) return default;
                if (!Tid.TryParse(x.PostId.RKey, out _)) return default;
                return new ParsedPostReference(rkey, this, x.PostId, x.PostRecord);
            })
                .Where(x => x != default)
                .ToArray();
        }
    }

    public record struct PostReference(string RKey, PostIdString PostId, Post? PostRecord = null);
    public record struct ParsedPostReference(Tid RKey, MergeablePostEnumerator Source, PostIdString PostId, Post? PostRecord = null)
    {
        public CollectionKind Kind => Source.Kind;
    }

    public enum CollectionKind
    {
        None,
        Posts,
        Reposts,
        Likes,
    }
}

