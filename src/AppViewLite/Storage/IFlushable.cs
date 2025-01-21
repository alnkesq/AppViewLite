using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Storage
{
    public interface IFlushable : IDisposable
    {
        public void Flush(bool disposing);
        public event EventHandler BeforeFlush;
    }
}

