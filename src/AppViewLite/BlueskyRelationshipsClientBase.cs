using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
            BlueskyRelationships.VerifyNotEnumerable<T>();
            lock (relationshipsUnlocked)
            {
                relationshipsUnlocked.EnsureNotDisposed();
                var result = func(relationshipsUnlocked);
                relationshipsUnlocked.MaybeGlobalFlush();
                return result;
            }
        }
        public void WithRelationshipsLock(Action<BlueskyRelationships> func)
        {
            lock (relationshipsUnlocked)
            {
                relationshipsUnlocked.EnsureNotDisposed();
                func(relationshipsUnlocked);
                relationshipsUnlocked.MaybeGlobalFlush();
            }
        }
    }
}

