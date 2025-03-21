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
    public abstract class BlueskyRelationshipsClientBase : LoggableBase
    {

        protected readonly BlueskyRelationships relationshipsUnlocked;
        public readonly PrimarySecondaryPair primarySecondaryPair;
        protected BlueskyRelationships? readOnlyReplicaRelationshipsUnlocked => primarySecondaryPair.readOnlyReplicaRelationshipsUnlocked;
        private readonly bool canUseSecondary;
        public BlueskyRelationshipsClientBase(PrimarySecondaryPair primarySecondaryPair)
        {
            this.relationshipsUnlocked = primarySecondaryPair.relationshipsUnlocked;
            this.primarySecondaryPair = primarySecondaryPair;
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



        
        public T WithRelationshipsLock<T>(Func<BlueskyRelationships, T> func, RequestContext ctx, bool urgent)
        {
            if (ctx == null) BlueskyRelationships.ThrowIncorrectLockUsageException("Missing ctx");
            BlueskyRelationships.VerifyNotEnumerable<T>();




            // We capture a new replica only due to age (very rare, since ReadOnlyReplicaMaxStalenessOnExplicitRead >> ReadOnlyReplicaMaxStalenessOpportunistic),
            // not due to version. If version is not sufficient, it's probably cheaper to use the primary than copying the whole queue.
            primarySecondaryPair.MaybeUpdateReadOnlyReplicaOnExplicitRead(minVersion: 0, alreadyHoldsLock: false);

            if (ctx.AllowStale && readOnlyReplicaRelationshipsUnlocked != null)
            {

                if (ctx.MinVersion > readOnlyReplicaRelationshipsUnlocked.Version)
                {
                    LogInfo("Performing read from primary for " + ctx.RequestUrl);
                    /* continue with primary instead */
                }
                else
                {
                    var minVersion = ctx.MinVersion;
                    var replica = this.readOnlyReplicaRelationshipsUnlocked;
                    replica.Lock.EnterReadLock();
                    Interlocked.Increment(ref ctx.ReadsFromSecondary);
                    var begin2 = PerformanceSnapshot.Capture();
                    try
                    {
                        replica.EnsureNotDisposed();
                        return func(replica);
                    }
                    finally
                    {
                        replica.Lock.ExitReadLock();
                        MaybeLogLongLockUsage(begin2, LockKind.SecondaryRead, ctx);
                    }
                }
            }


            BeforeLockEnter?.Invoke(ctx);

            if (urgent) 
            {
                var invoker = new object();

                var tcs = new TaskCompletionSource<object?>();
                ctx.TimeSpentWaitingForLocks.Start();
                var task = new UrgentReadTask((currentInvoker) => 
                {
                    ctx.TimeSpentWaitingForLocks.Stop();

                    if (invoker == currentInvoker) Interlocked.Increment(ref ctx.ReadsFromPrimaryLate);
                    else Interlocked.Increment(ref ctx.ReadsFromPrimaryStolen);

                    var begin = PerformanceSnapshot.Capture();
                    try
                    {
                        return func(relationshipsUnlocked);
                    }
                    finally
                    {
                        MaybeLogLongLockUsage(begin, LockKind.PrimaryReadUrgent, ctx);
                    }
                }, tcs);

                urgentReadTasks.Enqueue(task);

                Task.Run(() =>
                {
                    WithRelationshipsLock(rels =>
                    {
                        RunPendingUrgentReadTasks(invoker);
                    }, ctx, urgent: false);
                });

                return (T)tcs.Task.GetAwaiter().GetResult()!;
            }

            var rels = relationshipsUnlocked;


            rels.EnsureLockNotAlreadyHeld();
            ctx.TimeSpentWaitingForLocks.Start();
            rels.Lock.EnterReadLock();
            var restore = MaybeSetThreadName("Lock_Read");
            PerformanceSnapshot begin = default;
            try
            {
                ctx.TimeSpentWaitingForLocks.Stop();
                Interlocked.Increment(ref ctx.ReadsFromPrimaryNonUrgent);
                rels.EnsureNotDisposed();
                RunPendingUrgentReadTasks();

                begin = PerformanceSnapshot.Capture();
                var result = func(rels);
                return result;
            }
            finally
            {
                MaybeRestoreThreadName(restore);
                rels.Lock.ExitReadLock();

                MaybeLogLongLockUsage(begin, LockKind.PrimaryRead, ctx);
            }

        }

        private void RunPendingUrgentReadTasks(object? invoker = null)
        {
            while (urgentReadTasks.TryDequeue(out var urgentReadTask))
            {
                try
                {
                    urgentReadTask.Tcs.SetResult(urgentReadTask.Run(invoker));
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

            Interlocked.Increment(ref ctx.WriteOrUpgradeLockEnterCount);           

            relationshipsUnlocked.EnsureLockNotAlreadyHeld();
            relationshipsUnlocked.OnBeforeWriteLockEnter();
            BeforeLockEnter?.Invoke(ctx);
            ctx.TimeSpentWaitingForLocks.Start();
            using var _ = new ThreadPriorityScope(ThreadPriority.Normal);
            relationshipsUnlocked.Lock.EnterWriteLock();

            relationshipsUnlocked.ManagedThreadIdWithWriteLock = Environment.CurrentManagedThreadId;
            var restore = MaybeSetThreadName("**** LOCK_WRITE ****");
            PerformanceSnapshot begin = default;
            try
            {
                relationshipsUnlocked.Version++;
                ctx.BumpMinimumVersion(relationshipsUnlocked.Version);
                ctx.TimeSpentWaitingForLocks.Stop();
                relationshipsUnlocked.EnsureNotDisposed();
                RunPendingUrgentReadTasks();

                begin = PerformanceSnapshot.Capture();
                var result = func(relationshipsUnlocked);
                relationshipsUnlocked.MaybeGlobalFlush();
                primarySecondaryPair.MaybeUpdateReadOnlyReplicaOpportunistic(0, alreadyHoldsLock: true);
                return result;
            }
            finally
            {
                relationshipsUnlocked.ManagedThreadIdWithWriteLock = 0;
                MaybeRestoreThreadName(restore);
                relationshipsUnlocked.Lock.ExitWriteLock();

                MaybeLogLongLockUsage(begin, LockKind.PrimaryWrite, ctx);
            }
        }

        public T WithRelationshipsUpgradableLock<T>(Func<BlueskyRelationships, T> func, RequestContext ctx)
        {
            if (ctx == null) BlueskyRelationships.ThrowIncorrectLockUsageException("Missing ctx");
            BlueskyRelationships.VerifyNotEnumerable<T>();


            Interlocked.Increment(ref ctx.WriteOrUpgradeLockEnterCount);            

            relationshipsUnlocked.EnsureLockNotAlreadyHeld();
            BeforeLockEnter?.Invoke(ctx);
            ctx.TimeSpentWaitingForLocks.Start();
            relationshipsUnlocked.Lock.EnterUpgradeableReadLock();
            var restore = MaybeSetThreadName("**** LOCK_UPGRADEABLE ****");
            PerformanceSnapshot begin = default;
            try
            {
                ctx.TimeSpentWaitingForLocks.Stop();
                relationshipsUnlocked.EnsureNotDisposed();
                RunPendingUrgentReadTasks();

                begin = PerformanceSnapshot.Capture();
                var result = func(relationshipsUnlocked);
                return result;
            }
            finally
            {
                MaybeRestoreThreadName(restore);
                relationshipsUnlocked.Lock.ExitUpgradeableReadLock();
                MaybeLogLongLockUsage(begin, LockKind.PrimaryUpgradeable, ctx);
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

        //public readonly static int PrintLongReadLocksThreshold = AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_PRINT_LONG_READ_LOCKS_MS) ?? 30;
        //public readonly static int PrintLongWriteLocksThreshold = AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_PRINT_LONG_WRITE_LOCKS_MS) ?? PrintLongReadLocksThreshold;
        //public readonly static int PrintLongUpgradeableLocksThreshold = AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_PRINT_LONG_UPGRADEABLE_LOCKS_MS) ?? PrintLongWriteLocksThreshold;

        public readonly static TimeSpan PrintLongPrimaryLocks = TimeSpan.FromMilliseconds(AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_LONG_LOCK_PRIMARY_MS) ?? 30);
        public readonly static TimeSpan PrintLongSecondaryLocks = TimeSpan.FromMilliseconds(AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_LONG_LOCK_SECONDARY_MS) ?? 50);


        private static readonly double StopwatchTickConversionFactor = TimeSpan.TicksPerSecond / (double)Stopwatch.Frequency;
        public static TimeSpan StopwatchTicksToTimespan(long stopwatchTicks) => new TimeSpan((long)(stopwatchTicks * StopwatchTickConversionFactor));

        private static void MaybeLogLongLockUsage(PerformanceSnapshot begin, LockKind lockKind, RequestContext ctx)
        {
            if (begin == default) return;
            
            var end = PerformanceSnapshot.Capture();

            var maxGcGeneration =
                begin.Gc2Count != end.Gc2Count ? 2 :
                begin.Gc1Count != end.Gc1Count ? 1 :
                begin.Gc0Count != end.Gc0Count ? 0 :
                -1;

            var elapsedStopwatchTicks = end.StopwatchTicks - begin.StopwatchTicks;

            // not thread safe, but not important
            ctx.MaxOccurredGarbageCollectionGenerationInsideLock = Math.Max(ctx.MaxOccurredGarbageCollectionGenerationInsideLock, maxGcGeneration);

            var isSecondary = lockKind == LockKind.SecondaryRead;

            
            long totalStopwatchTicks;
            if (isSecondary)
                totalStopwatchTicks = Interlocked.Add(ref ctx.StopwatchTicksSpentInsideSecondaryLock, elapsedStopwatchTicks);
            else
                totalStopwatchTicks = Interlocked.Add(ref ctx.StopwatchTicksSpentInsidePrimaryLock, elapsedStopwatchTicks);

            var isAboveThreshold = StopwatchTicksToTimespan(totalStopwatchTicks) >= (isSecondary ? PrintLongSecondaryLocks : PrintLongPrimaryLocks);
            if (ctx.IsUrgent || isAboveThreshold)
                ctx.AddToMetricsTable();

            if (maxGcGeneration != -1) return;


            if (isAboveThreshold && false)
            {



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

                var elapsed = StopwatchTicksToTimespan(elapsedStopwatchTicks);
 
                LogInfo("Time spent inside the "+ lockKind +" lock: " + elapsed.TotalMilliseconds.ToString("0.0") + " ms" + (maxGcGeneration != -1 ? $" (includes {maxGcGeneration}gen GCs)" : null) + " " + ctx.DebugText);
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
                        LogInfo("    " + typeName + "." + method.Name + " (" + frame.GetFileName() + ":" + frame.GetFileLineNumber() + ")");
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



        public static ThreadPriorityScope CreateIngestionThreadPriorityScope() => new ThreadPriorityScope(ThreadPriority.Lowest);

    }

    public enum LockKind
    { 
        None,
        PrimaryRead,
        PrimaryWrite,
        PrimaryUpgradeable,
        PrimaryReadUrgent,
        SecondaryRead,
    }

    public record UrgentReadTask(Func<object?, object?> Run, TaskCompletionSource<object?> Tcs);
}

