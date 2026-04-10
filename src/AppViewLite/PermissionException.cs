using System;

namespace AppViewLite
{
    public class PermissionException : Exception
    {
        public PermissionException(string? message) : base(message)
        {
        }

        public PermissionException(string? message, Exception? innerException) : base(message, innerException)
        {
        }
    }
}

