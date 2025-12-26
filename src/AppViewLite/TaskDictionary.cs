using AppViewLite;
using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace AppViewLite
{
    public class TaskDictionary<TKey, TExtraArgs> where TKey : notnull
    {
        private TaskDictionary<TKey, TExtraArgs, byte> inner;
        public TaskDictionary(Func<TKey, TExtraArgs, Task> compute)
        {
            inner = new TaskDictionary<TKey, TExtraArgs, byte>(async (key, extraArgs) =>
            {
                await compute(key, extraArgs);
                return 0;
            });
        }

        public TaskDictionary(Func<TKey, TExtraArgs, Task> compute, TimeSpan evictTime)
        {
            inner = new TaskDictionary<TKey, TExtraArgs, byte>(async (key, extraArgs) =>
            {
                await compute(key, extraArgs);
                return 0;
            }, evictTime);
        }

        public Task GetTaskAsync(TKey key, TExtraArgs extraArgs)
        {
            return inner.GetValueAsync(key, extraArgs);
        }

        public void StartAsync(TKey key, TExtraArgs extraArgs, Action<Task>? onCompleted = null)
        {
            inner.StartAsync(key, extraArgs, onCompleted);
        }

        public bool TryGetTask(TKey key, out Task? task)
        {
            task = null;
            if (inner.TryGetTask(key, out var r))
            {
                task = r;
                return true;
            }
            return false;
        }
        public bool TryGetExtraArgs(TKey key, out TExtraArgs? extraArgs)
        {
            return inner.TryGetExtraArgs(key, out extraArgs);
        }

        public int Count => inner.Count;

        public bool Remove(TKey key) => inner.Remove(key);

    }
    public class TaskDictionary<TKey, TExtraArgs, TValue> where TKey : notnull
    {
        private readonly Func<TKey, TExtraArgs, Task<TValue>> compute;
        private readonly TimeSpan EvictTime;
        private readonly ConcurrentDictionary<TKey, (TExtraArgs ExtraArgs, Lazy<Task<TValue>> LazyTask)> dict = new();
        public TaskDictionary(Func<TKey, TExtraArgs, Task<TValue>> compute)
            : this(compute, TimeSpan.FromSeconds(30))
        {

        }
        public TaskDictionary(Func<TKey, TExtraArgs, Task<TValue>> compute, TimeSpan evictTime)
        {
            this.compute = compute;
            this.EvictTime = evictTime;
        }

        public Task<TValue> GetValueAsync(TKey key, TExtraArgs extraArgs)
        {
            var lazy = new Lazy<Task<TValue>>(() =>
            {
                return CreateTaskAsync(key, extraArgs);
            }, System.Threading.LazyThreadSafetyMode.ExecutionAndPublication);

            return dict.GetOrAdd(key, (extraArgs, lazy)).LazyTask.Value;

        }

        public int Count => dict.Count;

        public bool Remove(TKey key) => dict.TryRemove(key, out _);

        private static async Task YieldAsTask() => await Task.Yield();
        private async Task<TValue> CreateTaskAsync(TKey key, TExtraArgs extraArgs)
        {
            await YieldAsTask().ConfigureAwait(false); // gets out of the lock, and allows us to store early exceptions
            try
            {
                return await compute(key, extraArgs);
            }
            catch (Exception ex)
            {
                if (ex is OperationCanceledException && BlueskyEnrichedApis.Instance.ShutdownRequested.IsCancellationRequested)
                {
                    // We're shutting down, so this is expected: don't log.
                }
                else
                {
                    LoggableBase.LogLowImportanceException($"TaskDictionary error for {key}", ex);
                }
                throw;
            }
            finally
            {
                Task.Delay(EvictTime).GetAwaiter().OnCompleted(() =>
                {
                    dict.TryRemove(key, out _);
                });
            }
        }

        public void StartAsync(TKey key, TExtraArgs extraArgs, Action<Task<TValue>>? onCompleted = null)
        {
            var task = GetValueAsync(key, extraArgs);
            if (onCompleted != null)
            {
                task.ContinueWith(onCompleted).FireAndForget();
            }
        }

        public bool TryGetTask(TKey key, out Task<TValue>? result)
        {
            result = null;
            if (dict.TryGetValue(key, out var r))
            {
                result = r.LazyTask.Value;
                return true;
            }
            return false;
        }
        public bool TryGetExtraArgs(TKey key, out TExtraArgs? result)
        {
            result = default;
            if (dict.TryGetValue(key, out var r))
            {
                result = r.ExtraArgs;
                return true;
            }
            return false;
        }
    }
}

