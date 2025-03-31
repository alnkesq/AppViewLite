using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace AppViewLite
{
    public sealed class DedicatedThreadPoolScheduler : TaskScheduler, IDisposable
    {

        private readonly BlockingCollection<Task> tasks = new();
        private readonly List<Thread> threads;

        protected override IEnumerable<Task>? GetScheduledTasks() => tasks.ToArray();

        public event Action? BeforeTaskEnqueued;
        public event Action? AfterTaskProcessed;

        protected override void QueueTask(Task task)
        {
            BeforeTaskEnqueued?.Invoke();
            tasks.Add(task);
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
                TryExecuteTask(task);
                AfterTaskProcessed?.Invoke();
            }
        }

    }


}

