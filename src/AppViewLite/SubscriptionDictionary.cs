using AppViewLite.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    public class SubscriptionDictionary<TKey, TDelegate> where TDelegate: Delegate where TKey: notnull
    {
        private readonly ConcurrentDictionary<TKey, TDelegate> subscriptions = new();

        public void MaybeNotifyOutsideLock(TKey key, Action<TDelegate> invoke)
        {
            if (subscriptions.TryGetValue(key, out var handler))
            {
                BlueskyEnrichedApis.Instance.DispatchOutsideTheLock(() => invoke(handler));
            }
        }
        public void MaybeFetchDataAndNotifyOutsideLock<TDataToFetch>(TKey key, Func<TDataToFetch> fetchData, Action<TDataToFetch, TDelegate> invoke)
        {
            if (subscriptions.TryGetValue(key, out var handler))
            {
                var data = fetchData();
                BlueskyEnrichedApis.Instance.DispatchOutsideTheLock(() => invoke(data, handler));
            }
        }
        public void Unsubscribe(TKey key, TDelegate? handler)
        {
            if (handler == null) return;
            if (!subscriptions.TryRemove(new(key, handler)))
            {
                subscriptions.AddOrUpdate(key, handler, (_, prev) => (TDelegate)Delegate.Remove(prev, handler)!);
            }
        }

        public void Subscribe(TKey postId, TDelegate? handler)
        {
            if (handler == null) return;
            subscriptions.AddOrUpdate(postId, handler, (_, prev) => (TDelegate)Delegate.Combine(prev, handler));
        }
    }
}

