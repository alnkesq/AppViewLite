using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace AppViewLite
{

    public sealed class DedicatedThreadPoolScheduler : TaskScheduler, IDisposable
    {
        public readonly static int ThreadpoolBackpressure = AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_FIREHOSE_THREADPOOL_BACKPRESSURE) ?? 2000;
        private readonly BlockingCollection<Task> tasksNonSuspendable = new(ThreadpoolBackpressure);
        private readonly List<Thread> threads;
        protected override IEnumerable<Task>? GetScheduledTasks() => tasksNonSuspendable.ToArray();
        public event Action? BeforeTaskEnqueued;
        public event Action? AfterTaskProcessed;

        private long TotalNonSuspendableTasks;

        public long TotalTasksHijackedToSystemScheduler;

        protected override void QueueTask(Task task)
        {
            if (IsDedicatedThreadPoolSchedulerThread != 0)
            {
                // We must not block, othewise we can deadlock.

                BeforeTaskEnqueued?.Invoke();
                if (tasksNonSuspendable.TryAdd(task, 1000))
                {
                    Interlocked.Increment(ref TotalNonSuspendableTasks);
                }
                else
                {
                    Interlocked.Increment(ref TotalTasksHijackedToSystemScheduler);
                    Task.Factory.StartNew(
                        () => TryExecuteTask(task),
                        CancellationToken.None,
                        TaskCreationOptions.DenyChildAttach,
                        TaskScheduler.Default);
                }
            }
            else
            {
                BeforeTaskEnqueued?.Invoke();
                Interlocked.Increment(ref TotalNonSuspendableTasks);
                tasksNonSuspendable.Add(task);
            }

        }

        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => false;

        public void Dispose()
        {
            tasksNonSuspendable.CompleteAdding();
            foreach (var thread in threads)
            {
                thread.Join();
            }
        }


        public DedicatedThreadPoolScheduler(int threadCount, string? name)
        {
            TotalThreads = threadCount;
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


        public int BusyThreads;
        public int TotalThreads;
        public DateTime LastProcessedNonSuspendableTask;

        private void ThreadWorker()
        {
            try
            {
                ThreadWorkerCore();
            }
            catch (Exception ex)
            {
                BlueskyRelationships.ThrowFatalError("One of the worker threads of DedicatedThreadPoolScheduler died: " + ex);
            }
        
        }

        [ThreadStatic]
        private static int IsDedicatedThreadPoolSchedulerThread;

        private void ThreadWorkerCore()
        {
            IsDedicatedThreadPoolSchedulerThread++;
            try
            {

                using var scope = BlueskyRelationshipsClientBase.CreateIngestionThreadPriorityScope();
                while (true)
                {
                    if (!tasksNonSuspendable.TryTake(out var task, Timeout.Infinite))
                    {
                        Interlocked.Decrement(ref TotalThreads);
                        BlueskyRelationships.Assert(tasksNonSuspendable.IsAddingCompleted);
                        return;
                    }

                    Interlocked.Increment(ref BusyThreads);

                    if (!TryExecuteTask(task!))
                        BlueskyRelationships.ThrowFatalError("DedicatedThreadPoolScheduler: a task taken from the BlockingCollection of non-suspendable queued tasks was found to be already executed or running.");
                    LastProcessedNonSuspendableTask = DateTime.UtcNow;
                    AfterTaskProcessed?.Invoke();

                    Interlocked.Decrement(ref BusyThreads);
                }
            }
            finally
            {
                IsDedicatedThreadPoolSchedulerThread--;
            }
        }

        public object GetCountersThreadSafe()
        {
            return new
            {
                PendingNonSuspendableTasks = tasksNonSuspendable.Count,
                TotalNonSuspendableTasks,
                LastProcessedNonSuspendableTaskAgo = DateTime.UtcNow - LastProcessedNonSuspendableTask,
                TotalTasksHijackedToSystemScheduler,
                BusyThreads,
                TotalThreads,
            };
        }
    }


}

