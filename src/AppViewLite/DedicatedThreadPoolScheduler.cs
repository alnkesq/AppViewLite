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


        private readonly BlockingCollection<(Task Task, bool Suspendable)> tasks = new(AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_FIREHOSE_THREADPOOL_BACKPRESSURE) ?? 20_000);
        private readonly List<Thread> threads;

        protected override IEnumerable<Task>? GetScheduledTasks() => tasks.Select(x => x.Task).ToArray();

        public event Action? BeforeTaskEnqueued;
        public event Action? AfterTaskProcessed;
        private volatile BlockingCollection<Task>? pauseBuffer;

        private long uncompletedSuspendableTasks;

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
            QueueTask(task, canBeSuspended);

        }
        private void QueueTask(Task task, bool canBeSuspended)
        {

            BeforeTaskEnqueued?.Invoke();
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
                Interlocked.Increment(ref uncompletedSuspendableTasks);
            tasks.Add((task, canBeSuspended));

        }

        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => false;

        public void Dispose()
        {
            tasks.CompleteAdding();
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
            foreach (var task in tasks.GetConsumingEnumerable())
            {

                TryExecuteTask(task.Task);
                if (task.Suspendable)
                    Interlocked.Decrement(ref uncompletedSuspendableTasks);

                AfterTaskProcessed?.Invoke();
            }
        }

        internal Action PauseAndDrain()
        {
            BlueskyRelationships.Assert(pauseBuffer == null);

            pauseBuffer = new();
            var delayMs = 1;
            while (true)
            {
                var remaining = Interlocked.Read(ref uncompletedSuspendableTasks);
                LoggableBase.Log($"Draining firehose scheduler, remaining: {remaining}, waiting {(remaining == 0 ? 0 : delayMs)} ms, pause buffer: {pauseBuffer.Count}");
                if (remaining == 0) break;

                Thread.Sleep(delayMs);
                delayMs += Math.Max(1, delayMs / 4);
                delayMs = Math.Min(delayMs, 500);
            }
            BlueskyRelationships.Assert(!tasks.Any(x => x.Suspendable));
            return () =>
            {
                LoggableBase.Log("Resuming firehose scheduler");
                BlueskyRelationships.Assert(uncompletedSuspendableTasks == 0);
                BlueskyRelationships.Assert(!tasks.Any(x => x.Suspendable));
                var p = pauseBuffer;
                pauseBuffer = null;
                p.CompleteAdding();
                var reenqueuedTasks = 0;
                foreach (var task in p.GetConsumingEnumerable())
                {
                    QueueTask(task, canBeSuspended: true);
                    reenqueuedTasks++;
                }
                LoggableBase.Log($"Reenqueued {reenqueuedTasks} previously paused firehose scheduler tasks.");
            };
        }


    }


}

