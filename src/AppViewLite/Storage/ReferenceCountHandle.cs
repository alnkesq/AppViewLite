using System;
using System.Threading;

namespace AppViewLite.Storage
{

    internal class ReferenceCountManager<T> where T : IDisposable
    {
        internal int _refCount;
        internal T _value;
        public ReferenceCountManager(T value)
        {
            this._value = value;
        }
    }

    public class ReferenceCountHandle<T> : IDisposable where T: IDisposable
    {
        public ReferenceCountHandle(T value)
            :this(new ReferenceCountManager<T>(value), value)
        {
        }
        internal ReferenceCountHandle(ReferenceCountManager<T> manager, T value)
        {
            ArgumentNullException.ThrowIfNull(manager);
            this._manager = manager;
            this._value = value;
            Interlocked.Increment(ref _manager._refCount);
        }
        private ReferenceCountManager<T> _manager;
        private T _value;
        private int _disposed;
        public T Value
        {
            get
            {
                if (_disposed != 0) throw new ObjectDisposedException(nameof(ReferenceCountHandle<T>));
                return _value;
            }
        }
        public void Dispose()
        {
            if (Interlocked.Increment(ref _disposed) == 1)
            {
                if (Interlocked.Decrement(ref _manager._refCount) == 0)
                {
                    _manager._value.Dispose();
                }
                _value = default!;
                _manager = null!;
            }
        }

        public ReferenceCountHandle<T> AddRef()
        {
            return new ReferenceCountHandle<T>(_manager, _value);
        }

        public override string ToString()
        {
            return _disposed != 0 ? "(Disposed)" : ("[" + _manager._refCount + "] " + _value?.ToString());
        }
    }
}

