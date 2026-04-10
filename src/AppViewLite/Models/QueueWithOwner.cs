using System.Collections.Generic;

namespace AppViewLite.Models
{
    public class QueueWithOwner<T> : Queue<T>
    {
        public Plc Owner;
    }
}

