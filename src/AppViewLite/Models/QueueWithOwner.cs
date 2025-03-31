using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public class QueueWithOwner<T> : Queue<T>
    {
        public Plc Owner;
    }
}

