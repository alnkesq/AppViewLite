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
        public bool RemoteEnumerationExhausted;
        private Func<Tid, Task<PostReference[]>> RetrieveAsync;
        public MergeablePostEnumerator(Tid lastReturnedTid, Func<Tid, Task<PostReference[]>> retrieve, CollectionKind kind)
        {
            this.LastReturnedTid = lastReturnedTid;
            this.RetrieveAsync = retrieve;
            this.Kind = kind;
        }
        public CollectionKind Kind;

        public Queue<ParsedPostReference> enqueued = [];
        private Tid oldestEnqueued = Tid.MaxValue;

        public async Task<ParsedPostReference[]> GetNextPageAsync()
        {
            if (LastReturnedTid == default) return [];


            while (enqueued.TryPeek(out var queued) && queued.RKey.CompareTo(LastReturnedTid) >= 0)
            {
                enqueued.Dequeue();
            }

            if (enqueued.Count == 0 && !RemoteEnumerationExhausted)
            {
                var resumeFrom = new Tid(Math.Min(LastReturnedTid.TidValue, oldestEnqueued.TidValue));
                Console.Error.WriteLine("FETCHING " + Kind + " from " + (resumeFrom != Tid.MaxValue ? resumeFrom.Date.ToString() : "(now)"));
                var newitems = (await RetrieveAsync(resumeFrom)).Select(x =>
                {
                    if (!Tid.TryParse(x.RKey, out var rkey)) return default;
                    if (!Tid.TryParse(x.PostId.RKey, out _)) return default;
                    return new ParsedPostReference(rkey, this, x.PostId, x.PostRecord);
                });
                var any = false;
                foreach (var newitem in newitems)
                {
                    any = true;
                    if (newitem != default)
                    {
                        enqueued.Enqueue(newitem);
                        oldestEnqueued = newitem.RKey;
                    }
                }
                if (!any) RemoteEnumerationExhausted = true;
            }
            else
            {
                Console.Error.WriteLine("No need to fetch new items for " + Kind);
            }
            return enqueued.ToArray();
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

