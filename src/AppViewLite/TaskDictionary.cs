using AppViewLite;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    public class TaskDictionary<TKey> where TKey : notnull
    {
        private TaskDictionary<TKey, byte> inner;
        public TaskDictionary(Func<TKey, Task> compute)
        {
            inner = new TaskDictionary<TKey, byte>(async key => 
            {
                await compute(key);
                return 0;
            });
        }

        public TaskDictionary(Func<TKey, Task> compute, TimeSpan evictTime)
        {
            inner = new TaskDictionary<TKey, byte>(async key =>
            {
                await compute(key);
                return 0;
            }, evictTime);
        }

        public Task GetTaskAsync(TKey key)
        {
            return inner.GetValueAsync(key);
        }

        public void StartAsync(TKey key, Action<Task>? onCompleted = null)
        {
            inner.StartAsync(key, onCompleted);
        }

    }
    public class TaskDictionary<TKey, TValue> where TKey: notnull
    {
        private readonly Func<TKey, Task<TValue>> compute;
        private readonly TimeSpan EvictTime;
        private readonly ConcurrentDictionary<TKey, Lazy<Task<TValue>>> dict = new();
        public TaskDictionary(Func<TKey, Task<TValue>> compute)
            : this(compute, TimeSpan.FromSeconds(30))
        { 
        
        }
        public TaskDictionary(Func<TKey, Task<TValue>> compute, TimeSpan evictTime)
        {
            this.compute = compute;
            this.EvictTime = evictTime;
        }
        
        public Task<TValue> GetValueAsync(TKey key)
        {
            var lazy = new Lazy<Task<TValue>>(() =>
            {
                var p = CreateTaskAsync(key);
                return p;
            }, System.Threading.LazyThreadSafetyMode.ExecutionAndPublication);

            return dict.GetOrAdd(key, lazy).Value;
            
        }


        private async Task<TValue> CreateTaskAsync(TKey key)
        {
            await Task.Yield(); // gets out of the lock, and allows us to store early exceptions
            try
            {
                return await compute(key);
            }
            catch(Exception ex)
            {
                Console.Error.WriteLine($"TaskDictionary error for {key}: " + ex.Message);
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

        public void StartAsync(TKey key, Action<Task<TValue>>? onCompleted = null)
        {
            var task = GetValueAsync(key);
            if (onCompleted != null)
            {
                task.ContinueWith(onCompleted).FireAndForget();
            }
        }
    }
}

