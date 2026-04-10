using System;

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

