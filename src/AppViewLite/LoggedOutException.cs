using System;

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

