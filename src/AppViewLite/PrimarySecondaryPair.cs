using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AppViewLite
{
    public class PrimarySecondaryPair : IDisposable
    {
        public readonly BlueskyRelationships relationshipsUnlocked;
        public BlueskyRelationships? readOnlyReplicaRelationshipsUnlocked;
        public Lock buildNewReadOnlyReplicaLock = new Lock();

        internal ConcurrentQueue<UrgentPrimaryTask> urgentReadTasks = new();
        internal ConcurrentQueue<UrgentPrimaryTask> urgentWriteTasks = new();
        public PrimarySecondaryPair(BlueskyRelationships primary)
        {
            this.relationshipsUnlocked = primary;

            if (AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_USE_READONLY_REPLICA) ?? true)
            {
                relationshipsUnlocked.Lock.EnterReadLock();
                try
                {
                    this.readOnlyReplicaRelationshipsUnlocked = (BlueskyRelationships)relationshipsUnlocked.CloneAsReadOnly();
                }
                finally
                {
                    relationshipsUnlocked.Lock.ExitReadLock();
                }
            }
        }
        public readonly static TimeSpan ReadOnlyReplicaMaxStalenessOpportunistic = false && Debugger.IsAttached ? TimeSpan.FromHours(1) : TimeSpan.FromMilliseconds(AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_MAX_READONLY_STALENESS_MS_OPPORTUNISTIC) ?? 2000);
        public readonly static TimeSpan ReadOnlyReplicaMaxStalenessOnExplicitRead = false && Debugger.IsAttached ? TimeSpan.FromHours(2) : TimeSpan.FromMilliseconds(AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_MAX_READONLY_STALENESS_MS_EXPLICIT_READ) ?? 4000);

        public void MaybeUpdateReadOnlyReplicaOnExplicitRead(long minVersion)
        {
            MaybeUpdateReadOnlyReplica(minVersion, ReadOnlyReplicaMaxStalenessOnExplicitRead);
        }
        public void MaybeUpdateReadOnlyReplicaOpportunistic(long minVersion)
        {
            MaybeUpdateReadOnlyReplica(minVersion, ReadOnlyReplicaMaxStalenessOpportunistic);
        }

        public void MaybeUpdateReadOnlyReplica(long minVersion, TimeSpan maxStaleness)
        {
            // Must NOT hold lock (we take it ourselves, otherwise deadlock between the two locks)   
            var oldReplica = readOnlyReplicaRelationshipsUnlocked;
            if (oldReplica == null) return;

            var latestKnownVersion = relationshipsUnlocked.Version;

            if (!oldReplica!.IsAtLeastVersion(minVersion, maxStaleness, latestKnownVersion))
            {
                lock (buildNewReadOnlyReplicaLock)
                {
                    relationshipsUnlocked.Lock.EnterReadLock();
                    try
                    {
                        relationshipsUnlocked.EnsureNotDisposed();


                        oldReplica = readOnlyReplicaRelationshipsUnlocked!;
                        if (!oldReplica!.IsAtLeastVersion(minVersion, maxStaleness, latestKnownVersion))
                        {
                            this.readOnlyReplicaRelationshipsUnlocked = (BlueskyRelationships)relationshipsUnlocked.CloneAsReadOnly();
                            Task.Run(() => DisposeWhenNotInUse(oldReplica));
                        }

                    }
                    finally
                    {
                        relationshipsUnlocked.Lock.ExitReadLock();
                    }
                }
            }
        }

        private static void DisposeWhenNotInUse(BlueskyRelationships oldReplica)
        {
            var l = oldReplica.Lock;
            while (true)
            {
                Thread.Sleep(1000);
                var hasWaitingThreads = l.WaitingReadCount != 0 || l.WaitingUpgradeCount != 0 || l.WaitingWriteCount != 0;
                if (hasWaitingThreads)
                {
                    continue;
                }

                Thread.Sleep(1000);
                // any late readers past this point will throw. too late for them.
                oldReplica.Dispose(); // takes its own write lock
                try
                {
                    oldReplica.Lock.Dispose();
                }
                catch (SynchronizationLockException)
                {
                    // let the finalizer get rid of it
                }
                return;

            }
        }

        public void Dispose()
        {

            lock (buildNewReadOnlyReplicaLock)
            {
                readOnlyReplicaRelationshipsUnlocked?.Dispose();
                relationshipsUnlocked.Dispose();
            }
        }
    }
}

