using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    public class AssertionLiteException : Exception
    {
        private AssertionLiteException(string? message) : base(message)
        {
        }

        [DoesNotReturn]
        public static AssertionLiteException Throw(string message)
        {
            var ex = new AssertionLiteException(message);
            LoggableBase.Log("AssertionLiteException: " + ex.Message);
            LoggableBase.Log(new StackTrace(true).ToString());
            throw ex;
        }

        [DoesNotReturn]
        public static Exception ThrowBadEnumException<T>(T enumValue) where T : unmanaged
        {
            throw Throw("Unexpected enum value for " + enumValue.GetType() + ": " + enumValue);
        }
    }
}

