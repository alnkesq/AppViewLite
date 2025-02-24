using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Storage
{
    public interface ICloneableAsReadOnly
    {
        public ICloneableAsReadOnly CloneAsReadOnly();
    }
}

