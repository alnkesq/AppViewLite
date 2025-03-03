using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    public class LoggedOutException : Exception
    {
        public LoggedOutException(string message, Exception ex)
            : base(message, ex)
        {
        }
    }
}

