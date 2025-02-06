using AppViewLite.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
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

        public T WithRelationshipsLockForDid<T>(string did, Func<Plc, BlueskyRelationships, T> func)
        {
            return WithRelationshipsLockWithPreamble(rels =>
            {
                var plc = rels.TrySerializeDidMaybeReadOnly(did);
                if (plc == default) return PreambleResult<Plc>.NeedsWrite;
                return plc;
            }, func);
        }
        public void WithRelationshipsLockForDid(string did, Action<Plc, BlueskyRelationships> func)
        {
            WithRelationshipsLockWithPreamble(rels =>
            {
                var plc = rels.TrySerializeDidMaybeReadOnly(did);
                if (plc == default) return PreambleResult<Plc>.NeedsWrite;
                return plc;
            }, func);
        }

        public T WithRelationshipsLockWithPreamble<T>(Func<BlueskyRelationships, PreambleResult> preamble, Func<BlueskyRelationships, T> func)
        {
            return WithRelationshipsLockWithPreamble(rels =>
            {
                var preambleResult = preamble(rels);
                return preambleResult.Succeeded ? new PreambleResult<int>(1) : PreambleResult<int>.NeedsWrite;
            }, (_, rels) => func(rels));
        }

 
        public T WithRelationshipsLockWithPreamble<TPreamble, T>(Func<BlueskyRelationships, PreambleResult<TPreamble>> preamble, Func<TPreamble, BlueskyRelationships, T> func)
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
            });

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
            });
        }


        public void WithRelationshipsLockWithPreamble<TPreamble>(Func<BlueskyRelationships, PreambleResult<TPreamble>> preamble, Action<TPreamble, BlueskyRelationships> func)
        {
            WithRelationshipsLockWithPreamble(preamble, (p, rels) =>
            {
                func(p, rels);
                return 1;
            });
        }


        public T WithRelationshipsLock<T>(Func<BlueskyRelationships, T> func)
        {
            BlueskyRelationships.VerifyNotEnumerable<T>();

            Stopwatch? sw = null;
            int gc = 0;

            relationshipsUnlocked.EnsureLockNotAlreadyHeld();
            relationshipsUnlocked.Lock.EnterReadLock();
            try
            {
                relationshipsUnlocked.EnsureNotDisposed();
                sw = Stopwatch.StartNew();
                gc = GC.CollectionCount(0);

                var result = func(relationshipsUnlocked);
                return result;
            }
            finally
            {
                relationshipsUnlocked.Lock.ExitReadLock();

                MaybeLogLongLockUsage(sw, gc, LockKind.Read, PrintLongReadLocksThreshold);
            }

        }



        public T WithRelationshipsWriteLock<T>(Func<BlueskyRelationships, T> func)
        {
            BlueskyRelationships.VerifyNotEnumerable<T>();

            Stopwatch? sw = null;
            int gc = 0;

            relationshipsUnlocked.EnsureLockNotAlreadyHeld();
            relationshipsUnlocked.Lock.EnterWriteLock();
            try
            {
                relationshipsUnlocked.EnsureNotDisposed();
                sw = Stopwatch.StartNew();
                gc = GC.CollectionCount(0);

                var result = func(relationshipsUnlocked);
                relationshipsUnlocked.MaybeGlobalFlush();
                return result;
            }
            finally
            {
                relationshipsUnlocked.Lock.ExitWriteLock();

                MaybeLogLongLockUsage(sw, gc, LockKind.Write, PrintLongWriteLocksThreshold);
            }

        }

        public T WithRelationshipsUpgradableLock<T>(Func<BlueskyRelationships, T> func)
        {
            BlueskyRelationships.VerifyNotEnumerable<T>();

            Stopwatch? sw = null;
            int gc = 0;

            relationshipsUnlocked.EnsureLockNotAlreadyHeld();
            relationshipsUnlocked.Lock.EnterUpgradeableReadLock();
            try
            {
                relationshipsUnlocked.EnsureNotDisposed();
                sw = Stopwatch.StartNew();
                gc = GC.CollectionCount(0);

                var result = func(relationshipsUnlocked);
                return result;
            }
            finally
            {
                relationshipsUnlocked.Lock.ExitUpgradeableReadLock();
                MaybeLogLongLockUsage(sw, gc, LockKind.Upgradeable, PrintLongUpgradeableLocksThreshold);
            }

        }

        public void WithRelationshipsLock(Action<BlueskyRelationships> func)
        {
            WithRelationshipsLock(rels =>
            {
                func(rels);
                return false;
            });
        }
        public void WithRelationshipsWriteLock(Action<BlueskyRelationships> func)
        {
            WithRelationshipsWriteLock(rels =>
            {
                func(rels);
                return false;
            });
        }
        public void WithRelationshipsUpgradableLock(Action<BlueskyRelationships> func)
        {
            WithRelationshipsUpgradableLock(rels =>
            {
                func(rels);
                return false;
            });
        }

        public readonly static int PrintLongReadLocksThreshold = AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_PRINT_LONG_READ_LOCKS_MS) ?? 30;
        public readonly static int PrintLongWriteLocksThreshold = AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_PRINT_LONG_WRITE_LOCKS_MS) ?? PrintLongReadLocksThreshold;
        public readonly static int PrintLongUpgradeableLocksThreshold = AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_PRINT_LONG_UPGRADEABLE_LOCKS_MS) ?? PrintLongWriteLocksThreshold;

        private static void MaybeLogLongLockUsage(Stopwatch? sw, int prevGcCount, LockKind lockKind, int threshold)
        {
            if (sw == null) return;

            if (sw.ElapsedMilliseconds > threshold)
            {
                sw.Stop();
                var hadGcs = GC.CollectionCount(0) - prevGcCount;
                if (hadGcs != 0) return;
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
                Console.Error.WriteLine("Time spent inside the "+ lockKind +" lock: " + sw.ElapsedMilliseconds.ToString("0.0") + " ms" + (hadGcs != 0 ? $" (includes {hadGcs} GCs)" : null));
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
}

