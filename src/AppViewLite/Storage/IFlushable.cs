using System;
using System.ComponentModel;

namespace AppViewLite.Storage
{
    public interface IFlushable : IDisposable
    {
        public void DisposeNoFlush();
        public void Flush(bool disposing);
        public event EventHandler? BeforeFlush;
        public event EventHandler? AfterFlush;
        public event EventHandler<CancelEventArgs>? ShouldFlush;
        public event EventHandler? BeforeWrite;
        public event EventHandler? AfterCompactation;
    }
}

