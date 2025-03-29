using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace AppViewLite
{
    public sealed class DedicatedThreadPoolScheduler : TaskScheduler, IDisposable
    {
        public static TaskFactory? DefaultTaskFactory { get; set; }

        private readonly BlockingCollection<Task> tasks = new();
        private readonly List<Thread> threads;
        private readonly CancellationTokenSource cts = new();

        protected override IEnumerable<Task>? GetScheduledTasks() => tasks.ToArray();

        public event Action? BeforeTaskEnqueued;
        public event Action? AfterTaskProcessed;
        private readonly CancellationToken CancellationToken;

        protected override void QueueTask(Task task)
        {
            CancellationToken.ThrowIfCancellationRequested();
            BeforeTaskEnqueued?.Invoke();
            tasks.Add(task);
        }

        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => false;

        public void Dispose()
        {
            cts.Cancel();
            tasks.CompleteAdding();
            foreach (var thread in threads)
            {
                thread.Join();
            }
        }


        public DedicatedThreadPoolScheduler(int threadCount, string? name, CancellationToken ct)
        {
            CancellationToken = ct;
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
            foreach (var task in tasks.GetConsumingEnumerable(cts.Token))
            {
                if (CancellationToken.IsCancellationRequested) return;
                TryExecuteTask(task);
                AfterTaskProcessed?.Invoke();
            }
        }

    }


}

