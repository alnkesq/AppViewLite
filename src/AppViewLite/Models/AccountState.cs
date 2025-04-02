using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public enum AccountState : byte
    {
        Unknown = 0,
        Active = 1,


        NotActive = 10,
        TakenDown = 11,
        Suspended = 12,
        Deleted = 13,
        Deactivated = 14,
        Desynchronized = 15,
        Throttled = 16,
    }
}

