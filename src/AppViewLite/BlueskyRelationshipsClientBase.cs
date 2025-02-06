using AppViewLite.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
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
            return WithRelationshipsLockWithPreamble(rels => rels.SerializeDid(did), func);
        }
        public void WithRelationshipsLockForDid(string did, Action<Plc, BlueskyRelationships> func)
        {
            WithRelationshipsLockWithPreamble(rels => rels.SerializeDid(did), func);
        }

        public T WithRelationshipsLockWithPreamble<T>(Action<BlueskyRelationships> preamble, Func<BlueskyRelationships, T> func)
        {
            return WithRelationshipsLockWithPreamble(rels =>
            {
                preamble(rels);
                return false;
            }, (_, rels) => func(rels));
        }

        public T WithRelationshipsLockWithPreamble<TPreamble, T>(Func<BlueskyRelationships, TPreamble> preamble, Func<TPreamble, BlueskyRelationships, T> func)
        {
            return WithRelationshipsUpgradableLock(rels =>
            {
                var preambleResult = preamble(rels);
                rels.ForbidUpgrades++;
                try
                {
                    return func(preambleResult, rels);
                }
                finally
                {
                    rels.ForbidUpgrades--;
                }
            });
        }


        public void WithRelationshipsLockWithPreamble<TPreamble>(Func<BlueskyRelationships, TPreamble> preamble, Action<TPreamble, BlueskyRelationships> func)
        {
            WithRelationshipsLockWithPreamble(preamble, (p, rels) =>
            {
                func(p, rels);
                return false;
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

                MaybeLogLongLockUsage(sw, gc, isWriteLock: false);
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

                MaybeLogLongLockUsage(sw, gc, isWriteLock: true);
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
                MaybeLogLongLockUsage(sw, gc, false);
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

        public readonly static int PrintLongLocksThreshold = AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_PRINT_LONG_LOCKS_MS) ?? 30;
        private static void MaybeLogLongLockUsage(Stopwatch? sw, int prevGcCount, bool isWriteLock)
        {
            if (sw == null) return;
            if (sw.ElapsedMilliseconds > PrintLongLocksThreshold)
            {
                sw.Stop();
                var hadGcs = GC.CollectionCount(0) - prevGcCount;
                if (hadGcs != 0) return;
                var stack = new StackTrace(fNeedFileInfo: true);
                var frames = stack.GetFrames().ToList();
                while (frames.Count != 0)
                {
                    var ns = frames[^1].GetMethod()?.DeclaringType?.Namespace;
                    if (ns == null || !(ns.StartsWith("AppViewLite", StringComparison.Ordinal)))
                    {
                        frames.RemoveAt(frames.Count - 1);
                    }
                    else
                        break;
                }
                Console.Error.WriteLine("Time spent inside the "+ (isWriteLock ? "WRITE" : "READ") +" lock: " + sw.ElapsedMilliseconds.ToString("0.0") + " ms" + (hadGcs != 0 ? $" (includes {hadGcs} GCs)" : null));
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

    }
}

