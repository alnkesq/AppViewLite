using AppViewLite.Models;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace AppViewLite
{
    public abstract class BlueskyRelationshipsClientBase
    {

        protected readonly BlueskyRelationships relationshipsUnlocked;
        public BlueskyRelationshipsClientBase(BlueskyRelationships relationshipsUnlocked)
        {
            this.relationshipsUnlocked = relationshipsUnlocked;
        }

        public T WithRelationshipsLockForDid<T>(string did, Func<Plc, BlueskyRelationships, T> func, RequestContext? ctx)
        {
            return WithRelationshipsLockWithPreamble(rels =>
            {
                var plc = rels.TrySerializeDidMaybeReadOnly(did);
                if (plc == default) return PreambleResult<Plc>.NeedsWrite;
                return plc;
            }, func, ctx);
        }
        public void WithRelationshipsLockForDid(string did, Action<Plc, BlueskyRelationships> func, RequestContext? ctx = null)
        {
            WithRelationshipsLockWithPreamble(rels =>
            {
                var plc = rels.TrySerializeDidMaybeReadOnly(did);
                if (plc == default) return PreambleResult<Plc>.NeedsWrite;
                return plc;
            }, func, ctx);
        }
        public void WithRelationshipsLockForDids(string[] dids, Action<Plc[], BlueskyRelationships> func, RequestContext? ctx = null)
        {
            WithRelationshipsLockForDids(dids, (plcs, rels) => { func(plcs, rels); return 0; }, ctx);
        }
        public T WithRelationshipsLockForDids<T>(string[] dids, Func<Plc[], BlueskyRelationships, T> func, RequestContext? ctx = null)
        {
            return WithRelationshipsLockWithPreamble(rels =>
            {
                var result = new Plc[dids.Length];
                for (int i = 0; i < dids.Length; i++)
                {
                    var plc = rels.TrySerializeDidMaybeReadOnly(dids[i]);
                    if(plc == default) return PreambleResult<Plc[]>.NeedsWrite;
                    result[i] = plc;
                }
                return result;
            }, func, ctx);
        }

        public T WithRelationshipsLockWithPreamble<T>(Func<BlueskyRelationships, PreambleResult> preamble, Func<BlueskyRelationships, T> func, RequestContext? ctx = null)
        {
            return WithRelationshipsLockWithPreamble(rels =>
            {
                var preambleResult = preamble(rels);
                return preambleResult.Succeeded ? new PreambleResult<int>(1) : PreambleResult<int>.NeedsWrite;
            }, (_, rels) => func(rels), ctx);
        }

 
        public T WithRelationshipsLockWithPreamble<TPreamble, T>(Func<BlueskyRelationships, PreambleResult<TPreamble>> preamble, Func<TPreamble, BlueskyRelationships, T> func, RequestContext? ctx = null)
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


            return WithRelationshipsUpgradableLock(rels =>
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


        public void WithRelationshipsLockWithPreamble<TPreamble>(Func<BlueskyRelationships, PreambleResult<TPreamble>> preamble, Action<TPreamble, BlueskyRelationships> func, RequestContext? ctx = null)
        {
            WithRelationshipsLockWithPreamble(preamble, (p, rels) =>
            {
                func(p, rels);
                return 1;
            }, ctx);
        }


        private ConcurrentQueue<UrgentReadTask> urgentReadTasks = new();

        public T WithRelationshipsLock<T>(Func<BlueskyRelationships, T> func, RequestContext? ctx = null)
        {
            return WithRelationshipsLock(func, ctx, ctx?.IsUrgent == true);
        }
        public void WithRelationshipsLock(Action<BlueskyRelationships> func, RequestContext? ctx)
        {
            WithRelationshipsLock<int>(rels =>
            {
                func(rels);
                return 0;
            }, ctx);
        }
        public void WithRelationshipsLock(Action<BlueskyRelationships> func, RequestContext? ctx, bool urgent)
        {
            WithRelationshipsLock<int>(rels =>
            {
                func(rels);
                return 0;
            }, ctx, urgent);
        }

        public T WithRelationshipsLock<T>(Func<BlueskyRelationships, T> func, RequestContext? ctx, bool urgent)
        {
            BlueskyRelationships.VerifyNotEnumerable<T>();

            if (ctx != null)
                ctx.ReadLockEnterCount++;

            if (urgent) 
            {
                var tcs = new TaskCompletionSource<object?>();
                ctx?.TimeSpentWaitingForLocks?.Start();
                var task = new UrgentReadTask(() => 
                {
                    ctx?.TimeSpentWaitingForLocks?.Stop();
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


            Stopwatch? sw = null;
            int gc = 0;

            relationshipsUnlocked.EnsureLockNotAlreadyHeld();
            ctx?.TimeSpentWaitingForLocks?.Start();
            relationshipsUnlocked.Lock.EnterReadLock();
            var restore = MaybeSetThreadName("Lock_Read");
            try
            {
                ctx?.TimeSpentWaitingForLocks?.Stop();

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
                relationshipsUnlocked.Lock.ExitReadLock();

                MaybeLogLongLockUsage(sw, gc, LockKind.Read, PrintLongReadLocksThreshold, ctx);
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

        public T WithRelationshipsWriteLock<T>(Func<BlueskyRelationships, T> func, RequestContext? ctx = null, string? reason = null)
        {
            BlueskyRelationships.VerifyNotEnumerable<T>();

            Stopwatch? sw = null;
            int gc = 0;
            if (ctx != null)
                ctx.WriteOrUpgradeLockEnterCount++;

            relationshipsUnlocked.EnsureLockNotAlreadyHeld();
            ctx?.TimeSpentWaitingForLocks?.Start();
            relationshipsUnlocked.Lock.EnterWriteLock();

            BlueskyRelationships.ManagedThreadIdWithWriteLock = Environment.CurrentManagedThreadId;
            var restore = MaybeSetThreadName("**** LOCK_WRITE ****");
            try
            {
                ctx?.TimeSpentWaitingForLocks?.Stop();
                relationshipsUnlocked.EnsureNotDisposed();
                RunPendingUrgentReadTasks();
                sw = Stopwatch.StartNew();
                gc = GC.CollectionCount(0);

                var result = func(relationshipsUnlocked);
                relationshipsUnlocked.MaybeGlobalFlush();
                return result;
            }
            finally
            {
                BlueskyRelationships.ManagedThreadIdWithWriteLock = 0;
                MaybeRestoreThreadName(restore);
                relationshipsUnlocked.Lock.ExitWriteLock();

                MaybeLogLongLockUsage(sw, gc, LockKind.Write, PrintLongWriteLocksThreshold, ctx, reason);
            }
        }

        public T WithRelationshipsUpgradableLock<T>(Func<BlueskyRelationships, T> func, RequestContext? ctx = null)
        {
            BlueskyRelationships.VerifyNotEnumerable<T>();

            Stopwatch? sw = null;
            int gc = 0;

            if (ctx != null)
                ctx.WriteOrUpgradeLockEnterCount++;

            relationshipsUnlocked.EnsureLockNotAlreadyHeld();
            ctx?.TimeSpentWaitingForLocks?.Start();
            relationshipsUnlocked.Lock.EnterUpgradeableReadLock();
            var restore = MaybeSetThreadName("**** LOCK_UPGRADEABLE ****");
            try
            {
                ctx?.TimeSpentWaitingForLocks?.Stop();
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

        public void WithRelationshipsWriteLock(Action<BlueskyRelationships> func, RequestContext? ctx = null, string? reason = null)
        {
            WithRelationshipsWriteLock(rels =>
            {
                func(rels);
                return false;
            }, ctx, reason);
        }
        public void WithRelationshipsUpgradableLock(Action<BlueskyRelationships> func, RequestContext? ctx = null)
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

        private static void MaybeLogLongLockUsage(Stopwatch? sw, int prevGcCount, LockKind lockKind, int threshold, RequestContext? ctx, string? reason = null)
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
 
                Console.Error.WriteLine("Time spent inside the "+ lockKind +" lock: " + sw.ElapsedMilliseconds.ToString("0.0") + " ms" + (hadGcs != 0 ? $" (includes {hadGcs} GCs)" : null) + " " + reason +" " + ctx?.RequestUrl);
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
                var fullName = method.DeclaringType.ToString() + "::" + method.Name;
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

