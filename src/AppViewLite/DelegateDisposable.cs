using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    internal class DelegateDisposable : IDisposable
    {
        private Action? dispose;

        public DelegateDisposable(Action dispose)
        {
            this.dispose = dispose;
        }

        public void Dispose()
        {
            var d = dispose;
            if (d == null) return;
            dispose = null;
            d();
        }
    }
}

