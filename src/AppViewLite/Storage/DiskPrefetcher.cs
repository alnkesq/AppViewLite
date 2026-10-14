using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace AppViewLite.Storage
{
    public class DiskPrefetcher
    {
        public static DiskPrefetcher? Instance;

        private readonly List<Thread> threads;
        public DiskPrefetcher(int threadCount)
        {
            threads = new List<Thread>(threadCount);
            for (int i = 0; i < threadCount; i++)
            {
                var thread = new Thread(ThreadWorker) { IsBackground = true };
                thread.Name = "DiskPrefetcher";

                threads.Add(thread);
                thread.Start();
            }
        }

        public void Enqueue(DiskPrefetchTask task)
        {
            tasks.TryAdd(task);
        }

        private void ThreadWorker(object? obj)
        {
            try
            {
                ThreadWorkerCore();
            }
            catch (Exception ex)
            {
                CombinedPersistentMultiDictionary.Abort("One of the worker threads of DiskPrefetcher died: " + ex);
            }
        }

        private readonly BlockingCollection<DiskPrefetchTask> tasks = new();

        private void ThreadWorkerCore()
        {
            while (tasks.TryTake(out var task, Timeout.Infinite))
            {
                task.Prefetch(task);
            }
        }

        public int EnqueuedTasks => tasks.Count;
    }

    public class DiskPrefetchTask
    {
        public required Action<DiskPrefetchTask> Prefetch;
        private int _claimed;
        public readonly ManualResetEventSlim PrefetchCompleted = new();
        public bool TryClaim()
        {
            return Interlocked.Increment(ref _claimed) == 1;
        }
    }
}

