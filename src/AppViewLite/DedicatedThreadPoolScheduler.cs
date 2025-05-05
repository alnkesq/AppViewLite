using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace AppViewLite
{

    public sealed class DedicatedThreadPoolScheduler : TaskScheduler, IDisposable
    {


        private readonly BlockingCollection<Task> tasksSuspendable = new(AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_FIREHOSE_THREADPOOL_BACKPRESSURE) ?? 100_000);
        private readonly BlockingCollection<Task> tasksNonSuspendable = new();

        private readonly List<Thread> threads;

        protected override IEnumerable<Task>? GetScheduledTasks() => tasksNonSuspendable.Concat(tasksSuspendable).ToArray();

        public event Action? BeforeTaskEnqueued;
        public event Action? AfterTaskProcessed;
        private volatile BlockingCollection<Task>? pauseBuffer;

        private long UncompletedSuspendableTasks;
        private long TotalSuspendableTasks;
        private long TotalNonSuspendableTasks;

        [ThreadStatic] private static bool notifyTaskAboutToBeEnqueuedCanBeSuspended;
        internal static void NotifyTaskAboutToBeEnqueuedCanBeSuspended()
        {
            BlueskyRelationships.Assert(!notifyTaskAboutToBeEnqueuedCanBeSuspended);
            notifyTaskAboutToBeEnqueuedCanBeSuspended = true;
        }
        protected override void QueueTask(Task task)
        {
            var canBeSuspended = false;
            if (notifyTaskAboutToBeEnqueuedCanBeSuspended)
            {
                canBeSuspended = true;
                notifyTaskAboutToBeEnqueuedCanBeSuspended = false;
            }
            BeforeTaskEnqueued?.Invoke();
            QueueTaskCore(task, canBeSuspended);

        }
        private void QueueTaskCore(Task task, bool canBeSuspended)
        {

            var p = pauseBuffer;
            if (p != null && canBeSuspended)
            {
                try
                {
                    p.Add(task);
                    return;
                }
                catch (InvalidOperationException)
                {
                    // Between pauseBuffer read and Add(), the scheduler was resumed (CompleteAdding).
                    // Continue as usual.
                }
            }

            if (canBeSuspended)
            {
                Interlocked.Increment(ref UncompletedSuspendableTasks);
                Interlocked.Increment(ref TotalSuspendableTasks);
                tasksSuspendable.Add(task);
            }
            else
            {
                Interlocked.Increment(ref TotalNonSuspendableTasks);
                tasksNonSuspendable.Add(task);
            }

        }

        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => false;

        public void Dispose()
        {
            tasksNonSuspendable.CompleteAdding();
            tasksSuspendable.CompleteAdding();
            foreach (var thread in threads)
            {
                thread.Join();
            }
        }


        public DedicatedThreadPoolScheduler(int threadCount, string? name)
        {
            threads = new List<Thread>(threadCount);
            for (int i = 0; i < threadCount; i++)
            {
                var thread = new Thread(ThreadWorker) { IsBackground = true };
                if (name != null)
                {
                    thread.Name = name;
                }

                threads.Add(thread);
                thread.Start();
            }
        }

        private void ThreadWorker()
        {
            using var scope = BlueskyRelationshipsClientBase.CreateIngestionThreadPriorityScope();
            while (true)
            {
                var index = BlockingCollection<Task>.TryTakeFromAny([tasksNonSuspendable, tasksSuspendable], out var task, Timeout.Infinite);
                if (index == -1)
                {
                    BlueskyRelationships.Assert(tasksNonSuspendable.IsAddingCompleted && tasksSuspendable.IsAddingCompleted);
                    return;
                }
                if (index == 0)
                {
                    // non suspendable
                    if (!TryExecuteTask(task!))
                        BlueskyRelationships.ThrowFatalError("DedicatedThreadPoolScheduler: a task taken from the BlockingCollection of non-suspendable queued tasks was found to be already executed or running.");
                    AfterTaskProcessed?.Invoke();

                }
                else if (index == 1)
                {
                    // suspendable
                    if (!TryExecuteTask(task!))
                        BlueskyRelationships.ThrowFatalError("DedicatedThreadPoolScheduler: a task taken from the BlockingCollection of suspendable queued tasks was found to be already executed or running.");
                    Interlocked.Decrement(ref UncompletedSuspendableTasks);
                    AfterTaskProcessed?.Invoke();

                }
                else BlueskyRelationships.ThrowFatalError("Invalid index returned by TryTakeFromAny");
            }
        }

        internal Action PauseAndDrain()
        {
            BlueskyRelationships.Assert(pauseBuffer == null);

            pauseBuffer = new();
            var delayMs = 50;
            while (true)
            {
                var remaining = Interlocked.Read(ref UncompletedSuspendableTasks);
                LoggableBase.Log($"Draining firehose scheduler, remaining: {remaining}, waiting {(remaining == 0 ? 0 : delayMs)} ms, pause buffer: {pauseBuffer.Count}");
                if (remaining == 0) break;

                Thread.Sleep(delayMs);
                delayMs += Math.Max(1, delayMs / 4);
                delayMs = Math.Min(delayMs, 500);
            }
            BlueskyRelationships.Assert(tasksSuspendable.Count == 0);
            return () =>
            {
                LoggableBase.Log("Resuming firehose scheduler");
                BlueskyRelationships.Assert(UncompletedSuspendableTasks == 0);
                BlueskyRelationships.Assert(tasksSuspendable.Count == 0);
                var p = pauseBuffer;
                pauseBuffer = null;
                p.CompleteAdding();
                var reenqueuedTasks = 0;
                foreach (var task in p.GetConsumingEnumerable())
                {
                    QueueTaskCore(task, canBeSuspended: true);
                    reenqueuedTasks++;
                }
                LoggableBase.Log($"Reenqueued {reenqueuedTasks} previously paused firehose scheduler tasks.");
            };
        }

        public object GetCountersThreadSafe()
        {
            return new
            {
                PendingNonSuspendableTasks = tasksNonSuspendable.Count,
                PendingSuspendableTasks = tasksSuspendable.Count,
                PauseBuffer = pauseBuffer?.Count,
                UncompletedSuspendableTasks,
                TotalSuspendableTasks,
                TotalNonSuspendableTasks,
            };
        }
    }


}

