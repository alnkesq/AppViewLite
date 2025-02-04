using System;
using System.Collections.Generic;
using System.Diagnostics;
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
        public T WithRelationshipsLock<T>(Func<BlueskyRelationships, T> func)
        {
            lock (relationshipsUnlocked)
            {
                var sw = Stopwatch.StartNew();
                var gc = GC.CollectionCount(0);
                relationshipsUnlocked.EnsureNotDisposed();
                try
                {
                    var result = func(relationshipsUnlocked);
                    relationshipsUnlocked.MaybeGlobalFlush();
                    return result;
                }
                finally
                {
                    
                    MaybeLogLongLockUsage(sw, gc);
                }
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

        public readonly static int PrintLongLocksThreshold = AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_PRINT_LONG_LOCKS_MS) ?? 30;
        private static void MaybeLogLongLockUsage(Stopwatch sw, int prevGcCount)
        {
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
                Console.Error.WriteLine("Time spent inside the lock: " + sw.ElapsedMilliseconds.ToString("0.0") + " ms" + (hadGcs != 0 ? $" (includes {hadGcs} GCs)" : null));
                foreach (var frame in frames)
                {
                    var method = frame.GetMethod();
                    if (method != null)
                    {
                        if (method.Name is nameof(MaybeLogLongLockUsage) or nameof(WithRelationshipsLock)) continue;
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

