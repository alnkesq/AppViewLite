using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Storage
{
    public interface IFlushable : IDisposable
    {
        public void DisposeNoFlush();
        public void Flush(bool disposing);
        public event EventHandler BeforeFlush;
        public event EventHandler<CancelEventArgs> ShouldFlush;
    }
}

