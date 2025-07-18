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

        public RelationshipProbabilisticCache<TTarget>? RelationshipCache;

#nullable disable
        internal RelationshipDictionary()
        {
        }
#nullable restore
        public RelationshipDictionary(string baseDirectory, string prefix, Dictionary<string, SliceName[]> activeSlices, Func<TTarget, bool, UInt24?>? targetToApproxTarget = null, RelationshipProbabilisticCache<TTarget>? relationshipCache = null, Func<TTarget, MultiDictionaryIoPreference>? getCreationsIoPreferenceForKey = null, KeyProbabilisticCache<Relationship, DateTime>? deletionProbabilisticCache = null, bool zeroApproxTargetsAreValid = false)
        {
            if (!BlueskyRelationships.UseProbabilisticSets)
                relationshipCache = null;

            CombinedPersistentMultiDictionary<TKey, TValue> CreateMultiDictionary<TKey, TValue>(string suffix, PersistentDictionaryBehavior behavior = PersistentDictionaryBehavior.SortedValues, CombinedPersistentMultiDictionary<TKey, TValue>.CachedView[]? caches = null, Func<TKey, MultiDictionaryIoPreference>? getIoPreferenceForKey = null) where TKey : unmanaged, IComparable<TKey> where TValue : unmanaged, IComparable<TValue>, IEquatable<TValue>
            {
                return new CombinedPersistentMultiDictionary<TKey, TValue>(
                    Path.Combine(baseDirectory, prefix + suffix),
                    activeSlices.TryGetValue(prefix + suffix, out var active) ? active : [],
                    behavior,
                    caches,
                    getIoPreferenceForKey ?? BlueskyRelationships.GetIoPreferenceFunc<TKey>()
                    )
                { WriteBufferSize = BlueskyRelationships.TableWriteBufferSize };
            }
            this.RelationshipCache = relationshipCache;
            this.creations = CreateMultiDictionary<TTarget, Relationship>(string.Empty, caches: relationshipCache != null ? [relationshipCache] : null, getIoPreferenceForKey: getCreationsIoPreferenceForKey);
            this.deletions = CreateMultiDictionary<Relationship, DateTime>("-deletion", PersistentDictionaryBehavior.SingleValue, caches: deletionProbabilisticCache != null ? [deletionProbabilisticCache] : null);

            this.deletionCounts = CreateMultiDictionary<TTarget, int>("-deletion-counts-2", PersistentDictionaryBehavior.SingleValue);

            SetUpEventHandlers(creations);
            SetUpEventHandlers(deletions);
            SetUpEventHandlers(deletionCounts);

            this.targetToApproxTarget = targetToApproxTarget;
            if (targetToApproxTarget != null)
            {
                this.relationshipIdHashToApproxTarget = CreateMultiDictionary<RelationshipHash, UInt24>("-rkey-hash-to-approx-target24", PersistentDictionaryBehavior.SingleValue);
                this.relationshipIdHashToApproxTarget.DefaultValuesAreValid = zeroApproxTargetsAreValid;
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


        private HashSet<(TTarget Target, Plc Actor)>? TryGetNewRelationshipsSinceLastReadOnlySnapshot(long version)
        {
            if (NewRelationshipsSinceLastReadOnlySnapshot.MinVersion == version)
                return NewRelationshipsSinceLastReadOnlySnapshot.Value;
            if (NewRelationshipsSinceLastReadOnlySnapshotPrev.MinVersion == version)
                return NewRelationshipsSinceLastReadOnlySnapshotPrev.Value;
            return null;
        }

        public bool HasActor(TTarget target, Plc actor, out Relationship relationship, long knownAbsentAsOf = 0)
        {
            relationship = default;

            bool certainlyDoesntExist = false;
            if (knownAbsentAsOf != 0)
            {
                if (TryGetNewRelationshipsSinceLastReadOnlySnapshot(knownAbsentAsOf) is { } hs)
                {
                    if (!hs.Contains((target, actor)))
                    {
                        certainlyDoesntExist = true;
                        return false;
                    }
                }
            }

            var cannotPossiblyExist = false;
            if (RelationshipCache != null)
            {
                if (!RelationshipCache.PossiblyContains(target, actor))
                {
#if true
                    return false;
#else
                    if ((uint)(target.GetHashCode() ^ actor.GetHashCode()) % 256 > 2)
                    {
                        return false;
                    }
                    // In rare cases, proceed anyways, and fail assertion if we are wrong.
                    cannotPossiblyExist = true;                    
#endif
                }
            }

            if (creations.QueuedItems.TryGetValues(target, out var queuedGroup))
            {
                var latestRel = queuedGroup.ValuesSorted.LastOrDefault(x => x.Actor == actor);
                if (latestRel.Actor != default)
                {
                    if (cannotPossiblyExist)
                        BlueskyRelationships.ThrowFatalError("Probabilistic filter returned false, but relationship was actually found.");
                    if (IsDeleted(latestRel)) return false;
                    relationship = latestRel;

                    BlueskyRelationships.Assert(!certainlyDoesntExist);
                    return true;
                }
            }


            var chunks = creations.GetValuesChunkedLatestFirst(target, omitQueue: true);
            foreach (var chunk in chunks)
            {
                var span = chunk.AsSpan();

                var z = span.BinarySearch(new Relationship(actor, default));
                if (z >= 0) AssertionLiteException.Throw("Approximate item should not have been found.");
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

                    BlueskyRelationships.Assert(!certainlyDoesntExist);
                    return true;
                }
            }
            return false;
        }



        public TTarget? Delete(Relationship rel, DateTime deletionDate, TTarget target = default)
        {
            if (!IsDeleted(rel))
            {
                deletions.Add(rel, deletionDate);

                if (!TargetHasValue(target) && targetToApproxTarget != null)
                {
                    target = TryGetTarget(rel);
                }

                if (TargetHasValue(target))
                {
                    var prevDeletionCount = GetDeletionCount(target);
                    if (prevDeletionCount < 0) BlueskyRelationships.ThrowFatalError("GetDeletionCount() < 0");
                    deletionCounts.Add(target, prevDeletionCount + 1);
                }
            }
            return target;
        }

        private static bool TargetHasValue(TTarget target) => !EqualityComparer<TTarget>.Default.Equals(target, default);

        private static void EnsureValidTarget(TTarget target)
        {
            if (!TargetHasValue(target))
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

        public bool Add(TTarget target, Relationship relationship, long knownAbsentAsOf = 0)
        {
            EnsureValidTarget(target);

            if (HasActor(target, relationship.Actor, out var oldrel, knownAbsentAsOf))
            {
                if (oldrel == relationship) return false;
                Delete(oldrel, DateTime.UtcNow, target);
            }
            NewRelationshipsSinceLastReadOnlySnapshot.Value.Add((target, relationship.Actor));
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


        public TTarget TryGetTarget(Relationship rel)
        {
            if (relationshipIdHashToApproxTarget == null) return default;
            var relHash = GetRelationshipHash(rel);

            if (!relationshipIdHashToApproxTarget.TryGetSingleValue(relHash, out var approxTarget))
                return default;


            var relationshipValuesPreference = MultiDictionaryIoPreference.ValuesMmap | MultiDictionaryIoPreference.OffsetsMmap;
            creations.InitializeIoPreferenceForKey(default, ref relationshipValuesPreference);

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
                    var rels = slice.GetValues(i, relationshipValuesPreference);
                    if (ContainsRelationship(rels.Span.AsSmallSpan(), rel))
                        return k;
                }

                for (long i = z - 1; i >= 0; i--)
                {
                    var k = keySpan[i];
                    if (targetToApproxTarget!(k, false) != approxTarget)
                        break;
                    var rels = slice.GetValues(i, relationshipValuesPreference);
                    if (ContainsRelationship(rels.Span.AsSmallSpan(), rel))
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
            return ContainsRelationshipBinarySearch(relationships, rel);
        }
        private static bool ContainsRelationshipBinarySearch(ReadOnlySpan<Relationship> relationships, Relationship rel)
        {
            var z = relationships.BinarySearch(new Relationship(rel.Actor, default));
            if (z >= 0) AssertionLiteException.Throw("Approximate item should not have been found.");
            var indexOfNextLargest = ~z;

            if (indexOfNextLargest == relationships.Length) return false;

            for (int i = indexOfNextLargest; i < relationships.Length; i++)
            {
                if (relationships[i].Actor != rel.Actor) return false;
                if (relationships[i].RelationshipRKey == rel.RelationshipRKey) return true;
            }
            return false;
        }

        private readonly struct LambdaComparable<T, TApprox> : IComparable<T> where TApprox : struct, IComparable<TApprox> where T : struct
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

        public bool FlushIfNoPendingCompactations()
        {
            var ok = true;

            if (!creations.FlushIfNoPendingCompactations()) ok = false;
            if (!deletions.FlushIfNoPendingCompactations()) ok = false;
            if (!deletionCounts.FlushIfNoPendingCompactations()) ok = false;
            if (relationshipIdHashToApproxTarget != null)
            {
                if (!relationshipIdHashToApproxTarget.FlushIfNoPendingCompactations()) ok = false;
            }
            return ok;
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
            copy.RelationshipCache = this.RelationshipCache; // Reads during writes are safe.
            copy.relationshipIdHashToApproxTarget = this.relationshipIdHashToApproxTarget?.CloneAsReadOnly();
            copy.targetToApproxTarget = this.targetToApproxTarget;
            copy._multidictionaries = new CombinedPersistentMultiDictionary?[] { copy.creations, copy.deletions, copy.deletionCounts, copy.relationshipIdHashToApproxTarget }.WhereNonNull().ToArray();

            var currentVersion = GetVersion!();

            NewRelationshipsSinceLastReadOnlySnapshotPrev = NewRelationshipsSinceLastReadOnlySnapshot;
            NewRelationshipsSinceLastReadOnlySnapshot = new(new(), currentVersion);
            return copy;
        }

        public Func<long>? GetVersion;
        private Versioned<HashSet<(TTarget Target, Plc Actor)>> NewRelationshipsSinceLastReadOnlySnapshot = new(new(), 0);
        private Versioned<HashSet<(TTarget Target, Plc Actor)>> NewRelationshipsSinceLastReadOnlySnapshotPrev = new(new(), 0);

        ICloneableAsReadOnly ICloneableAsReadOnly.CloneAsReadOnly() => CloneAsReadOnly();
    }
}

