using System;
using System.Threading;

namespace AppViewLite.Storage
{
    public ref struct ThreadPriorityScope : IDisposable
    {
        private readonly ThreadPriority prev;
        private bool disposed;
        public ThreadPriorityScope(ThreadPriority priority)
        {
            prev = Thread.CurrentThread.Priority;
            Thread.CurrentThread.Priority = priority;
        }
        public void Dispose()
        {
            if (!disposed)
            {
                Thread.CurrentThread.Priority = prev;
                disposed = true;
            }
        }
    }
}

