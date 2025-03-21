using AppViewLite.Models;
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
        protected readonly static bool UseProbabilisticSets = AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_USE_PROBABILISTIC_SETS) ?? true;

        public abstract IReadOnlyList<CombinedPersistentMultiDictionary> Multidictionaries { get; }
    }
    public class RelationshipDictionary<TTarget> : RelationshipDictionary, ICheckpointable, ICloneableAsReadOnly where TTarget : unmanaged, IComparable<TTarget>
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
        private IReadOnlyList<CombinedPersistentMultiDictionary> _multidictionaries;
        public override IReadOnlyList<CombinedPersistentMultiDictionary> Multidictionaries => _multidictionaries;

        private RelationshipProbabilisticCache<TTarget>? relationshipCache;

#nullable disable
        internal RelationshipDictionary()
        { 
        }
#nullable restore
        public RelationshipDictionary(string baseDirectory, string prefix, Dictionary<string, SliceName[]> activeSlices, Func<TTarget, bool, UInt24?>? targetToApproxTarget = null, RelationshipProbabilisticCache<TTarget>? relationshipCache = null)
        {
            if (!UseProbabilisticSets)
                relationshipCache = null;
            
            CombinedPersistentMultiDictionary<TKey, TValue> CreateMultiDictionary<TKey, TValue>(string suffix, PersistentDictionaryBehavior behavior = PersistentDictionaryBehavior.SortedValues, CombinedPersistentMultiDictionary<TKey, TValue>.CachedView[]? caches = null) where TKey : unmanaged, IComparable<TKey> where TValue : unmanaged, IComparable<TValue>, IEquatable<TValue>
            {
                return new CombinedPersistentMultiDictionary<TKey, TValue>(Path.Combine(baseDirectory, prefix + suffix), activeSlices.TryGetValue(prefix + suffix, out var active) ? active : [], behavior, caches) { WriteBufferSize = BlueskyRelationships.TableWriteBufferSize };
            }
            this.relationshipCache = relationshipCache;
            this.creations = CreateMultiDictionary<TTarget, Relationship>(string.Empty, caches: relationshipCache != null ? [relationshipCache] : null);
            this.deletions = CreateMultiDictionary<Relationship, DateTime>("-deletion", PersistentDictionaryBehavior.SingleValue);
            using var deletionCountsLegacy = CreateMultiDictionary<TTarget, int>("-deletion-counts", PersistentDictionaryBehavior.SortedValues);

            this.deletionCounts = CreateMultiDictionary<TTarget, int>("-deletion-counts-2", PersistentDictionaryBehavior.SingleValue);
            if (deletionCounts.GroupCount == 0)
            {
                foreach (var chunk in deletionCountsLegacy.EnumerateUnsortedGrouped())
                {
                    deletionCounts.Add(chunk.Key, chunk.Values[chunk.Values.Count - 1]);
                }
            }

            SetUpEventHandlers(creations);
            SetUpEventHandlers(deletions);
            SetUpEventHandlers(deletionCounts);

            this.targetToApproxTarget = targetToApproxTarget;
            if (targetToApproxTarget != null)
            {
                this.relationshipIdHashToApproxTarget = CreateMultiDictionary<RelationshipHash, UInt24>("-rkey-hash-to-approx-target24", PersistentDictionaryBehavior.SingleValue);
                SetUpEventHandlers(relationshipIdHashToApproxTarget);
            }
            _multidictionaries = new CombinedPersistentMultiDictionary?[] { creations, deletions, deletionCounts, relationshipIdHashToApproxTarget }.WhereNonNull().ToArray();
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

            var cannotPossiblyExist = false;
            if (relationshipCache != null)
            {
                if (!relationshipCache.PossiblyContains(target, actor))
                {
                    if ((uint)(target.GetHashCode() ^ actor.GetHashCode()) % 256 > 2)
                    {
                        return false;
                    }
                    // In rare cases, proceed anyways, and fail assertion if we are wrong.
                    cannotPossiblyExist = true;
                }
            }

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
                    if (cannotPossiblyExist)
                        BlueskyRelationships.ThrowFatalError("Probabilistic filter returned false, but relationship was actually found.");
                    return true;
                }
            }
            return false;
        }

    

        public TTarget? Delete(Relationship rel, DateTime deletionDate, TTarget? target = null)
        {
            if (target != null)
                EnsureValidTarget(target.Value);

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
                    if (prevDeletionCount < 0) BlueskyRelationships.ThrowFatalError("GetDeletionCount() < 0");
                    deletionCounts.Add(target.Value, prevDeletionCount + 1);
                }
            }
            return target;
        }

        private static void EnsureValidTarget(TTarget value)
        {
            if (EqualityComparer<TTarget>.Default.Equals(value, default))
                BlueskyRelationships.ThrowFatalError("target is default(TTarget)");
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

        public bool Add(TTarget target, Relationship relationship)
        {
            EnsureValidTarget(target);

            if (HasActor(target, relationship.Actor, out var oldrel))
            {
                if (oldrel == relationship) return false;
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
            return true;
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
                var z = slice.BinarySearch(new LambdaComparable<TTarget, UInt24>(approxTarget, x => targetToApproxTarget!(x, true)));
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
                    relationshipIdHashToApproxTarget!
                }.WhereNonNull();
        }

        public (string TableName, SliceName[] ActiveSlices)[] GetActiveSlices()
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

        public RelationshipDictionary<TTarget> CloneAsReadOnly()
        {
            var copy = new RelationshipDictionary<TTarget>();
            copy.creations = this.creations.CloneAsReadOnly();
            copy.deletions = this.deletions.CloneAsReadOnly();
            copy.deletionCounts = this.deletionCounts.CloneAsReadOnly();
            copy.relationshipIdHashToApproxTarget = this.relationshipIdHashToApproxTarget?.CloneAsReadOnly();
            copy.targetToApproxTarget = this.targetToApproxTarget;
            copy._multidictionaries = new CombinedPersistentMultiDictionary?[] { copy.creations, copy.deletions, copy.deletionCounts, copy.relationshipIdHashToApproxTarget }.WhereNonNull().ToArray();
            return copy;

        }

        ICloneableAsReadOnly ICloneableAsReadOnly.CloneAsReadOnly() => CloneAsReadOnly();
    }
}

