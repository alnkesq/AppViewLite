using AppViewLite.Models;
using AppViewLite.Storage;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace AppViewLite
{
    public abstract class BlueskyRelationshipsClientBase : IDisposable
    {

        protected readonly BlueskyRelationships relationshipsUnlocked;
        protected BlueskyRelationships? readOnlyReplicaRelationshipsUnlocked;
        private Lock buildNewReadOnlyReplicaLock = new Lock();
        public BlueskyRelationshipsClientBase(BlueskyRelationships relationshipsUnlocked, bool useReadOnlyReplica = false)
        {
            this.relationshipsUnlocked = relationshipsUnlocked;
            if (useReadOnlyReplica && (AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_USE_READONLY_REPLICA) ?? true))
            {
                this.relationshipsUnlocked.BeforeExitingLockUpgrade += (_, _) => MaybeUpdateReadOnlyReplica(0, ReadOnlyReplicaMaxStalenessOpportunistic, alreadyHoldsLock: true);
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


        public T WithRelationshipsLockForDid<T>(string did, Func<Plc, BlueskyRelationships, T> func, RequestContext ctx)
        {
            return WithRelationshipsLockWithPreamble(rels =>
            {
                var plc = rels.TrySerializeDidMaybeReadOnly(did, ctx);
                if (plc == default) return PreambleResult<Plc>.NeedsWrite;
                return plc;
            }, func, ctx);
        }
        public void WithRelationshipsLockForDid(string did, Action<Plc, BlueskyRelationships> func, RequestContext ctx)
        {
            WithRelationshipsLockWithPreamble(rels =>
            {
                var plc = rels.TrySerializeDidMaybeReadOnly(did, ctx);
                if (plc == default) return PreambleResult<Plc>.NeedsWrite;
                return plc;
            }, func, ctx);
        }
        public void WithRelationshipsLockForDids(string[] dids, Action<Plc[], BlueskyRelationships> func, RequestContext ctx)
        {
            WithRelationshipsLockForDids(dids, (plcs, rels) => { func(plcs, rels); return 0; }, ctx);
        }
        public T WithRelationshipsLockForDids<T>(string[] dids, Func<Plc[], BlueskyRelationships, T> func, RequestContext ctx)
        {
            return WithRelationshipsLockWithPreamble(rels =>
            {
                var result = new Plc[dids.Length];
                for (int i = 0; i < dids.Length; i++)
                {
                    var plc = rels.TrySerializeDidMaybeReadOnly(dids[i], ctx);
                    if(plc == default) return PreambleResult<Plc[]>.NeedsWrite;
                    result[i] = plc;
                }
                return result;
            }, func, ctx);
        }

        public T WithRelationshipsLockWithPreamble<T>(Func<BlueskyRelationships, PreambleResult> preamble, Func<BlueskyRelationships, T> func, RequestContext ctx)
        {
            return WithRelationshipsLockWithPreamble(rels =>
            {
                var preambleResult = preamble(rels);
                return preambleResult.Succeeded ? new PreambleResult<int>(1) : PreambleResult<int>.NeedsWrite;
            }, (_, rels) => func(rels), ctx);
        }

 
        public T WithRelationshipsLockWithPreamble<TPreamble, T>(Func<BlueskyRelationships, PreambleResult<TPreamble>> preamble, Func<TPreamble, BlueskyRelationships, T> func, RequestContext ctx)
        {
            // Preamble might be executed twice, if the first attempt in readonly mode fails.

            // We avoid entering upgradable, because only 1 thread can be upgradable at any given time.

            bool succeeded = false;
            var result = WithRelationshipsLock(rels =>
            {
                var preambleResult = preamble(rels);
                if (preambleResult.Succeeded)
                {
                    succeeded = true;
                    return func(preambleResult.Value, rels);
                }
                return default!;
            }, ctx);

            if (succeeded) return result;


            return WithRelationshipsWriteLock(rels =>
            {
                var preambleResult = preamble(rels);
                rels.ForbidUpgrades++;
                try
                {

                    return func(preambleResult.Value, rels);
                }
                finally
                {
                    rels.ForbidUpgrades--;
                }
            }, ctx);
        }


        public void WithRelationshipsLockWithPreamble<TPreamble>(Func<BlueskyRelationships, PreambleResult<TPreamble>> preamble, Action<TPreamble, BlueskyRelationships> func, RequestContext ctx)
        {
            WithRelationshipsLockWithPreamble(preamble, (p, rels) =>
            {
                func(p, rels);
                return 1;
            }, ctx);
        }


        private ConcurrentQueue<UrgentReadTask> urgentReadTasks = new();

        public T WithRelationshipsLock<T>(Func<BlueskyRelationships, T> func, RequestContext ctx)
        {
            return WithRelationshipsLock(func, ctx, ctx.IsUrgent);
        }
        public void WithRelationshipsLock(Action<BlueskyRelationships> func, RequestContext ctx)
        {
            WithRelationshipsLock<int>(rels =>
            {
                func(rels);
                return 0;
            }, ctx);
        }
        public void WithRelationshipsLock(Action<BlueskyRelationships> func, RequestContext ctx, bool urgent)
        {
            WithRelationshipsLock<int>(rels =>
            {
                func(rels);
                return 0;
            }, ctx, urgent);
        }



        private readonly static TimeSpan ReadOnlyReplicaMaxStalenessOpportunistic = false && Debugger.IsAttached ? TimeSpan.FromHours(1) : TimeSpan.FromMilliseconds(AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_MAX_READONLY_STALENESS_MS_OPPORTUNISTIC) ?? 2000);
        private readonly static TimeSpan ReadOnlyReplicaMaxStalenessOnExplicitRead = false && Debugger.IsAttached ? TimeSpan.FromHours(2) : TimeSpan.FromMilliseconds(AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_MAX_READONLY_STALENESS_MS_EXPLICIT_READ) ?? 4000);

        public T WithRelationshipsLock<T>(Func<BlueskyRelationships, T> func, RequestContext ctx, bool urgent)
        {
            if (ctx == null) BlueskyRelationships.ThrowIncorrectLockUsageException("Missing ctx");
            BlueskyRelationships.VerifyNotEnumerable<T>();




            // We capture a new replica only due to age (very rare, since ReadOnlyReplicaMaxStalenessOnExplicitRead >> ReadOnlyReplicaMaxStalenessOpportunistic),
            // not due to version. If version is not sufficient, it's probably cheaper to use the primary than copying the whole queue.
            MaybeUpdateReadOnlyReplica(minVersion: 0, ReadOnlyReplicaMaxStalenessOnExplicitRead, alreadyHoldsLock: false);

            if (ctx.AllowStale && readOnlyReplicaRelationshipsUnlocked != null)
            {

                if (ctx.MinVersion > readOnlyReplicaRelationshipsUnlocked.Version)
                {
                    Console.Error.WriteLine("Performing read from primary for " + ctx.RequestUrl);
                    /* continue with primary instead */
                }
                else
                {
                    var minVersion = ctx.MinVersion;
                    var replica = this.readOnlyReplicaRelationshipsUnlocked;
                    replica.Lock.EnterReadLock();
                    try
                    {
                        return func(replica);
                    }
                    finally
                    {
                        replica.Lock.ExitReadLock();
                    }
                }
            }



            ctx.ReadLockEnterCount++;
            ctx.AddToMetricsTable();
            


            BeforeLockEnter?.Invoke(ctx);

            if (urgent) 
            {
                var tcs = new TaskCompletionSource<object?>();
                ctx.TimeSpentWaitingForLocks.Start();
                var task = new UrgentReadTask(() => 
                {
                    ctx.TimeSpentWaitingForLocks.Stop();
                    return func(relationshipsUnlocked);
                }, tcs);

                urgentReadTasks.Enqueue(task);

                Task.Run(() =>
                {
                    WithRelationshipsLock(rels =>
                    {
                        RunPendingUrgentReadTasks();
                    }, ctx, urgent: false);
                });

                return (T)tcs.Task.GetAwaiter().GetResult()!;
            }

            var rels = relationshipsUnlocked;


            Stopwatch? sw = null;
            int gc = 0;

            rels.EnsureLockNotAlreadyHeld();
            ctx.TimeSpentWaitingForLocks.Start();
            rels.Lock.EnterReadLock();
            var restore = MaybeSetThreadName("Lock_Read");
            try
            {
                ctx.TimeSpentWaitingForLocks.Stop();

                rels.EnsureNotDisposed();
                RunPendingUrgentReadTasks();
                sw = Stopwatch.StartNew();
                gc = GC.CollectionCount(0);

                var result = func(rels);
                return result;
            }
            finally
            {
                MaybeRestoreThreadName(restore);
                rels.Lock.ExitReadLock();

                MaybeLogLongLockUsage(sw, gc, LockKind.Read, PrintLongReadLocksThreshold, ctx);
            }

        }

        private void MaybeUpdateReadOnlyReplica(long minVersion, TimeSpan maxStaleness, bool alreadyHoldsLock)
        {
            var oldReplica = readOnlyReplicaRelationshipsUnlocked;
            if (oldReplica == null) return;

            var latestKnownVersion = relationshipsUnlocked.Version;

            if (!oldReplica!.IsAtLeastVersion(minVersion, maxStaleness, latestKnownVersion))
            {

                if (!alreadyHoldsLock) relationshipsUnlocked.Lock.EnterReadLock();
                try
                {
                    lock (buildNewReadOnlyReplicaLock)
                    {
                        oldReplica = readOnlyReplicaRelationshipsUnlocked!;
                        if (!oldReplica!.IsAtLeastVersion(minVersion, maxStaleness, latestKnownVersion))
                        {
                            this.readOnlyReplicaRelationshipsUnlocked = (BlueskyRelationships)relationshipsUnlocked.CloneAsReadOnly();
                            Task.Run(() => DisposeWhenNotInUse(oldReplica));
                        }
                    }
                }
                finally
                {
                    if (!alreadyHoldsLock) relationshipsUnlocked.Lock.ExitReadLock();
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

        private void RunPendingUrgentReadTasks()
        {
            while (urgentReadTasks.TryDequeue(out var urgentReadTask))
            {
                try
                {
                    urgentReadTask.Tcs.SetResult(urgentReadTask.Run());
                }
                catch (Exception ex)
                {
                    urgentReadTask.Tcs.SetException(ex);
                }
            }
        }

        public Action<RequestContext>? BeforeLockEnter;

        public T WithRelationshipsWriteLock<T>(Func<BlueskyRelationships, T> func, RequestContext ctx)
        {
            if(ctx == null) BlueskyRelationships.ThrowIncorrectLockUsageException("Missing ctx");
            BlueskyRelationships.VerifyNotEnumerable<T>();

            Stopwatch? sw = null;
            int gc = 0;

            ctx.WriteOrUpgradeLockEnterCount++;
            ctx.AddToMetricsTable();
            

            relationshipsUnlocked.EnsureLockNotAlreadyHeld();
            relationshipsUnlocked.OnBeforeWriteLockEnter();
            BeforeLockEnter?.Invoke(ctx);
            ctx.TimeSpentWaitingForLocks.Start();
            using var _ = new ThreadPriorityScope(ThreadPriority.Normal);
            relationshipsUnlocked.Lock.EnterWriteLock();

            relationshipsUnlocked.ManagedThreadIdWithWriteLock = Environment.CurrentManagedThreadId;
            var restore = MaybeSetThreadName("**** LOCK_WRITE ****");
            try
            {
                relationshipsUnlocked.Version++;
                ctx.BumpMinimumVersion(relationshipsUnlocked.Version);
                ctx.TimeSpentWaitingForLocks.Stop();
                relationshipsUnlocked.EnsureNotDisposed();
                RunPendingUrgentReadTasks();
                sw = Stopwatch.StartNew();
                gc = GC.CollectionCount(0);

                var result = func(relationshipsUnlocked);
                relationshipsUnlocked.MaybeGlobalFlush();
                MaybeUpdateReadOnlyReplica(0, ReadOnlyReplicaMaxStalenessOpportunistic, alreadyHoldsLock: true);
                return result;
            }
            finally
            {
                relationshipsUnlocked.ManagedThreadIdWithWriteLock = 0;
                MaybeRestoreThreadName(restore);
                relationshipsUnlocked.Lock.ExitWriteLock();

                MaybeLogLongLockUsage(sw, gc, LockKind.Write, PrintLongWriteLocksThreshold, ctx);
            }
        }

        public T WithRelationshipsUpgradableLock<T>(Func<BlueskyRelationships, T> func, RequestContext ctx)
        {
            if (ctx == null) BlueskyRelationships.ThrowIncorrectLockUsageException("Missing ctx");
            BlueskyRelationships.VerifyNotEnumerable<T>();

            Stopwatch? sw = null;
            int gc = 0;


            ctx.WriteOrUpgradeLockEnterCount++;
            ctx.AddToMetricsTable();
            

            relationshipsUnlocked.EnsureLockNotAlreadyHeld();
            BeforeLockEnter?.Invoke(ctx);
            ctx.TimeSpentWaitingForLocks.Start();
            relationshipsUnlocked.Lock.EnterUpgradeableReadLock();
            var restore = MaybeSetThreadName("**** LOCK_UPGRADEABLE ****");
            try
            {
                ctx.TimeSpentWaitingForLocks.Stop();
                relationshipsUnlocked.EnsureNotDisposed();
                RunPendingUrgentReadTasks();
                sw = Stopwatch.StartNew();
                gc = GC.CollectionCount(0);

                var result = func(relationshipsUnlocked);
                return result;
            }
            finally
            {
                MaybeRestoreThreadName(restore);
                relationshipsUnlocked.Lock.ExitUpgradeableReadLock();
                MaybeLogLongLockUsage(sw, gc, LockKind.Upgradeable, PrintLongUpgradeableLocksThreshold, ctx);
            }

        }


        private static void MaybeRestoreThreadName(string? prevName)
        {
#if false
            Thread.CurrentThread.Name = prevName;
#endif
        }

        private static string? MaybeSetThreadName(string newName)
        {
#if false
            var prev = Thread.CurrentThread.Name;
            Thread.CurrentThread.Name = newName;
            return prev;
#else
            return null;
#endif

        }

        public void WithRelationshipsWriteLock(Action<BlueskyRelationships> func, RequestContext ctx)
        {
            WithRelationshipsWriteLock(rels =>
            {
                func(rels);
                return false;
            }, ctx);
        }
        public void WithRelationshipsUpgradableLock(Action<BlueskyRelationships> func, RequestContext ctx)
        {
            WithRelationshipsUpgradableLock(rels =>
            {
                func(rels);
                return false;
            }, ctx);
        }

        public readonly static int PrintLongReadLocksThreshold = AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_PRINT_LONG_READ_LOCKS_MS) ?? 30;
        public readonly static int PrintLongWriteLocksThreshold = AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_PRINT_LONG_WRITE_LOCKS_MS) ?? PrintLongReadLocksThreshold;
        public readonly static int PrintLongUpgradeableLocksThreshold = AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_PRINT_LONG_UPGRADEABLE_LOCKS_MS) ?? PrintLongWriteLocksThreshold;

        private static void MaybeLogLongLockUsage(Stopwatch? sw, int prevGcCount, LockKind lockKind, int threshold, RequestContext ctx, string? reason = null)
        {
            if (sw == null) return;

            if (sw.ElapsedMilliseconds > threshold)
            {
                sw.Stop();
                var hadGcs = GC.CollectionCount(0) - prevGcCount;
                if (hadGcs != 0) return;
                
                
                if (Debugger.IsAttached) return; // would pollute stderr when stepping


                var stack = new StackTrace(fNeedFileInfo: true);
                var frames = stack.GetFrames().ToList();
                while (frames.Count != 0)
                {
                    if (!IsAppViewLiteStackFrame(frames[^1].GetMethod()))
                    {
                        frames.RemoveAt(frames.Count - 1);
                    }
                    else
                        break;
                }
 
                Console.Error.WriteLine("Time spent inside the "+ lockKind +" lock: " + sw.ElapsedMilliseconds.ToString("0.0") + " ms" + (hadGcs != 0 ? $" (includes {hadGcs} GCs)" : null) + " " + reason +" " + ctx.RequestUrl);
                foreach (var frame in frames)
                {
                    var method = frame.GetMethod();
                    if (method != null)
                    {
                        if (method.DeclaringType == typeof(BlueskyRelationshipsClientBase))
                        {
                            if (method.Name is 
                                nameof(MaybeLogLongLockUsage) or
                                nameof(WithRelationshipsLock) or
                                nameof(WithRelationshipsWriteLock) or
                                nameof(WithRelationshipsLockForDid) or
                                nameof(WithRelationshipsLockWithPreamble) or
                                nameof(WithRelationshipsUpgradableLock)
                                ) continue;
                        }

                        var type = method.DeclaringType;
                        var typeName = type?.ToString();
                        if (typeName is
                             "System.Runtime.CompilerServices.AsyncMethodBuilderCore" or
                             "System.Runtime.CompilerServices.AsyncTaskMethodBuilder`1[TResult]" or
                             "System.Runtime.CompilerServices.AsyncTaskMethodBuilder`1+AsyncStateMachineBox`1[TResult,TStateMachine]" or
                             "System.Threading.ExecutionContext"
                         ) continue;
                        Console.Error.WriteLine("    " + typeName + "." + method.Name + " (" + frame.GetFileName() + ":" + frame.GetFileLineNumber() + ")");
                    }
                }
            }
        }

        private static bool IsAppViewLiteStackFrame(MethodBase? method)
        {
            if (method == null) return false;

            var ns = method.DeclaringType?.Namespace;
            if (ns == null) return false;

            if (ns.StartsWith("AppViewLite", StringComparison.Ordinal))
            {
                var fullName = method.DeclaringType!.ToString() + "::" + method.Name;
                if (fullName.Contains("Main", StringComparison.Ordinal) && fullName.Contains("Program"))
                {
                    // very noisy ASP.NET stacks
                    // ...
                    //Microsoft.AspNetCore.Cors.Infrastructure.CorsMiddleware.Invoke
                    //AppViewLite.Web.Program+<>c+<<Main>b__5_5>d.MoveNext
                    //AppViewLite.Web.Program+<>c.<Main>b__5_5
                    return false;
                }
                return true;
            }
            return false;
        }

        public void Dispose()
        {
            readOnlyReplicaRelationshipsUnlocked?.Dispose();
        }


        public static ThreadPriorityScope CreateIngestionThreadPriorityScope() => new ThreadPriorityScope(ThreadPriority.Lowest);
    }

    public enum LockKind
    { 
        None,
        Read,
        Write,
        Upgradeable,
    }

    public record UrgentReadTask(Func<object?> Run, TaskCompletionSource<object?> Tcs);
}

