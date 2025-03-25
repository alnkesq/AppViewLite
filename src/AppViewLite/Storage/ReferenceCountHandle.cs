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

    public class ReferenceCountHandle<T> : IDisposable where T : IDisposable
    {
        public ReferenceCountHandle(T value)
        {
            this._manager = new ReferenceCountManager<T>(value) { _refCount = 1 };
            this._value = value;
        }
        internal ReferenceCountHandle(ReferenceCountManager<T> manager, T value)
        {
            if (manager == null) throw new ObjectDisposedException(null);

            while (true)
            {
                var prevRefCount = manager._refCount;
                if (prevRefCount == 0) throw new ObjectDisposedException(null);
                var updatedRefCount = prevRefCount + 1;
                if (prevRefCount == Interlocked.CompareExchange(ref manager._refCount, updatedRefCount, prevRefCount))
                    break;

            }

            this._manager = manager;
            this._value = value;
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
                var updatedRefCount = Interlocked.Decrement(ref _manager._refCount);
                if (updatedRefCount < 0)
                    Environment.FailFast("Negative updatedRefCount");

                if (updatedRefCount == 0)
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

