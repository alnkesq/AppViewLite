using AppViewLite.Models;
using AppViewLite;
using AppViewLite.Storage;
using AppViewLite.Numerics;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;

namespace AppViewLite
{
    public abstract class RelationshipDictionary
    { 
        public abstract IReadOnlyList<CombinedPersistentMultiDictionary> Multidictionaries { get; }
    }
    public class RelationshipDictionary<TTarget> : RelationshipDictionary, ICheckpointable where TTarget : unmanaged, IComparable<TTarget>
    {

        internal CombinedPersistentMultiDictionary<TTarget, Relationship> creations;
        internal CombinedPersistentMultiDictionary<Relationship, DateTime> deletions;
        private CombinedPersistentMultiDictionary<TTarget, int> deletionCounts;
        private CombinedPersistentMultiDictionary<RelationshipHash, UInt24>? relationshipIdHashToApproxTarget;
        private Func<TTarget, bool, UInt24?>? targetToApproxTarget;
        public event EventHandler? BeforeFlush;
        public event EventHandler? AfterFlush;
        public event EventHandler<CancelEventArgs>? ShouldFlush;
        public event EventHandler? BeforeWrite;
        public override IReadOnlyList<CombinedPersistentMultiDictionary> Multidictionaries { get; }
        public RelationshipDictionary(string baseDirectory, string prefix, Dictionary<string, string[]> activeSlices, Func<TTarget, bool, UInt24?>? targetToApproxTarget = null)
        {
            CombinedPersistentMultiDictionary<TKey, TValue> CreateMultiDictionary<TKey, TValue>(string suffix, PersistentDictionaryBehavior behavior = PersistentDictionaryBehavior.SortedValues) where TKey : unmanaged, IComparable<TKey> where TValue : unmanaged, IComparable<TValue>, IEquatable<TValue>
            {
                return new CombinedPersistentMultiDictionary<TKey, TValue>(Path.Combine(baseDirectory, prefix + suffix), activeSlices.TryGetValue(prefix + suffix, out var active) ? active : [], behavior) { WriteBufferSize = BlueskyRelationships.TableWriteBufferSize };
            }
            this.creations = CreateMultiDictionary<TTarget, Relationship>(string.Empty);
            this.deletions = CreateMultiDictionary<Relationship, DateTime>("-deletion", PersistentDictionaryBehavior.SingleValue);
            this.deletionCounts = CreateMultiDictionary<TTarget, int>("-deletion-counts", PersistentDictionaryBehavior.SortedValues);
            this.deletionCounts.OnCompactation = z => new[] { z.Max() };
            SetUpEventHandlers(creations);
            SetUpEventHandlers(deletions);
            SetUpEventHandlers(deletionCounts);

            this.targetToApproxTarget = targetToApproxTarget;
            if (targetToApproxTarget != null)
            {
                this.relationshipIdHashToApproxTarget = CreateMultiDictionary<RelationshipHash, UInt24>("-rkey-hash-to-approx-target24", PersistentDictionaryBehavior.SingleValue);
                SetUpEventHandlers(relationshipIdHashToApproxTarget);
            }
            Multidictionaries = new CombinedPersistentMultiDictionary?[] { creations, deletions, deletionCounts, relationshipIdHashToApproxTarget }.Where(x => x != null).ToArray()!;
        }

        private void SetUpEventHandlers(IFlushable inner)
        {
            inner.BeforeFlush += OnBeforeFlush;
            inner.BeforeWrite += OnBeforeWrite;
            inner.ShouldFlush += OnShouldFlush;
            inner.AfterFlush += OnAfterFlush;
        }

        private void OnBeforeFlush(object? sender, EventArgs e)
        {
            BeforeFlush?.Invoke(this, e);
        }
        private void OnAfterFlush(object? sender, EventArgs e)
        {
            AfterFlush?.Invoke(this, e);
        }

        private void OnBeforeWrite(object? sender, EventArgs e)
        {
            BeforeWrite?.Invoke(this, e);
        }
        private void OnShouldFlush(object? sender, CancelEventArgs e)
        {
            ShouldFlush?.Invoke(this, e);
        }

        public long GetActorCount(TTarget target)
        {
            var c = creations.GetValueCount(target);
            var deletionCount = GetDeletionCount(target);
            return c - deletionCount;
        }

        public bool HasAtLeastActorCount(TTarget target, long minimum)
        {
            var c = creations.GetValueCount(target);
            if (c < minimum) return false;

            var deletionCount = GetDeletionCount(target);
            return c - deletionCount >= minimum;
        }

        public IEnumerable<Relationship> GetRelationshipsSorted(TTarget target, Relationship continuation)
        {

            foreach (var r in creations.GetValuesSorted(target, continuation != default ? continuation : null))
            {
                if (IsDeleted(r))
                    continue;
                yield return r;
            }
            
        }

        public bool IsDeleted(Relationship relationship) => deletions.ContainsKey(relationship);


        public bool HasActor(TTarget target, Plc actor, out Relationship relationship)
        {
            relationship = default;
            var chunks = creations.GetValuesChunkedLatestFirst(target);
            foreach (var chunk in chunks)
            {
                var span = chunk.AsSpan();

                var z = span.BinarySearch(new Relationship(actor, default));
                if (z >= 0) throw new Exception();
                var indexOfNextLargest = ~z;

                if (indexOfNextLargest == span.Length) continue;
                var next = span[indexOfNextLargest];
                if (next.Actor == actor)
                {
                    var i = indexOfNextLargest;

                    // We fetch the latest relationship for this Key-Actor pair.
                    while (i + 1 < span.Length && span[i + 1].Actor == actor)
                    {
                        next = span[++i];
                    }
                    if (IsDeleted(next))
                        return false;
                    relationship = next;
                    return true;
                }
            }
            return false;
        }

    

        public TTarget? Delete(Relationship rel, DateTime deletionDate, TTarget? target = null)
        {
            if (!IsDeleted(rel))
            {
                deletions.Add(rel, deletionDate);

                if (target == null && targetToApproxTarget != null)
                {
                    target = GetTarget(rel);
                }

                if (target != null)
                {
                    var prevDeletionCount = GetDeletionCount(target.Value);
                    deletionCounts.Add(target.Value, prevDeletionCount + 1);
                }
            }
            return target;
        }



        public int GetDeletionCount(TTarget target)
        {
            foreach (var chunk in deletionCounts.GetValuesChunkedLatestFirst(target))
            {
                var span = chunk.AsSpan();
                return span[span.Length - 1];
            }
            return 0;
        }

        public void Add(TTarget target, Relationship relationship)
        {
            if (HasActor(target, relationship.Actor, out var oldrel))
            {
                if (oldrel == relationship) return;
                Delete(oldrel, DateTime.UtcNow, target);
            }
            creations.Add(target, relationship);

            if (relationshipIdHashToApproxTarget != null)
            {
                //if (typeof(TTarget) == typeof(PostIdTimeFirst))
                //{
                //    var t = (PostIdTimeFirst)(object)target;
                //    var timeDelta = relationship.RelationshipRKey.Date - t.PostRKey.Date;
                //    if (timeDelta > TimeSpan.Zero && timeDelta < TimeSpan.FromMinutes(40))
                //        return;
                //}
                // TODO: can we avoid saving it if like date and post date are close in time?


                var approxTarget = targetToApproxTarget!(target, false);
                if (approxTarget != null)
                {
                    relationshipIdHashToApproxTarget.Add(GetRelationshipHash(relationship), approxTarget.Value);
                }
            }
        }


        public TTarget GetTarget(Relationship rel)
        {
            if (relationshipIdHashToApproxTarget == null) return default;
            var relHash = GetRelationshipHash(rel);

            if (!relationshipIdHashToApproxTarget.TryGetSingleValue(relHash, out var approxTarget))
                return default;

            foreach (var sliceTuple in creations.slices)
            {
                var slice = sliceTuple.Reader;

                var keySpan = slice.Keys.Span;
                var z = slice.BinarySearch(new LambdaComparable<TTarget, UInt24>(approxTarget, x => targetToApproxTarget(x, true)));
                if (z < 0) continue;

                for (long i = z; i < keySpan.Length; i++)
                {
                    var k = keySpan[i];
                    if (targetToApproxTarget!(k, false) != approxTarget)
                        break;
                    var rels = slice.GetValues(i);
                    if (ContainsRelationship(rels.Span.AsSmallSpan, rel))
                        return k;
                }

                for (long i = z - 1; i >= 0; i--)
                {
                    var k = keySpan[i];
                    if (targetToApproxTarget!(k, false) != approxTarget)
                        break;
                    var rels = slice.GetValues(i);
                    if (ContainsRelationship(rels.Span.AsSmallSpan, rel))
                        return k;

                }
            }

            foreach (var group in creations.QueuedItems)
            {
                if (group.Values.Contains(rel))
                    return group.Key;
            }

            return default;
        }
        private static bool ContainsRelationship(ReadOnlySpan<Relationship> relationships, Relationship rel)
        {
            var contains = ContainsRelationshipBinarySearch(relationships, rel);
            //if (contains != relationships.Contains(rel)) throw new Exception();
            return contains;
        }
        private static bool ContainsRelationshipBinarySearch(ReadOnlySpan<Relationship> relationships, Relationship rel)
        {
            var z = relationships.BinarySearch(new Relationship(rel.Actor, default));
            if (z >= 0) throw new Exception();
            var indexOfNextLargest = ~z;

            if (indexOfNextLargest == relationships.Length) return false;

            for (int i = indexOfNextLargest; i < relationships.Length; i++)
            {
                if (relationships[i].Actor != rel.Actor) return false;
                if (relationships[i].RelationshipRKey == rel.RelationshipRKey) return true;
            }
            return false;
        }

        private readonly struct LambdaComparable<T, TApprox> : IComparable<T> where TApprox: struct, IComparable<TApprox> where T:struct
        {
            private readonly TApprox Approx;
            private readonly Func<T, TApprox?> TargetToApproxTarget;


            public LambdaComparable(TApprox approxTarget, Func<T, TApprox?> targetToApproxTarget)
            {
                this.Approx = approxTarget;
                this.TargetToApproxTarget = targetToApproxTarget;
            }

            public int CompareTo(T other)
            {
                var otherApprox = TargetToApproxTarget(other!);
                return Approx.CompareTo(otherApprox!.Value);
            }
        }
        public static RelationshipHash GetRelationshipHash(Relationship rel)
        {
            var likeHash = System.IO.Hashing.XxHash64.HashToUInt64(MemoryMarshal.AsBytes<Relationship>(new ReadOnlySpan<Relationship>(in rel)));
            return new RelationshipHash((uint)likeHash, (ushort)(likeHash >> 32));
        }

        public void Flush(bool disposing)
        {
            creations.Flush(disposing);
            deletions.Flush(disposing);
            deletionCounts.Flush(disposing);
            relationshipIdHashToApproxTarget?.Flush(disposing);
        }

        public long GetApproximateActorCount(TTarget key)
        {
            return creations.GetValueCount(key);
        }

        private IEnumerable<ICheckpointable> GetComponents()
        {
            return new ICheckpointable[]
                {
                    creations,
                    deletions,
                    deletionCounts,
                    relationshipIdHashToApproxTarget
                }.Where(x => x != null);
        }

        public (string TableName, string[] ActiveSlices)[] GetActiveSlices()
        {
            return GetComponents().SelectMany(x => x.GetActiveSlices()).ToArray();
        }

        public void Dispose()
        {
            foreach (var component in GetComponents())
            {
                component.Dispose();
            }
        }
        public void DisposeNoFlush()
        {
            foreach (var component in GetComponents())
            {
                component.DisposeNoFlush();
            }
        }
    }
}

