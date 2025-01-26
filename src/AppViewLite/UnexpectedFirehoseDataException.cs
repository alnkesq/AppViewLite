using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    public class UnexpectedFirehoseDataException : Exception
    {
        public UnexpectedFirehoseDataException(string? message) : base(message)
        {
        }

        public UnexpectedFirehoseDataException(string? message, Exception? innerException) : base(message, innerException)
        {
        }
    }
}

