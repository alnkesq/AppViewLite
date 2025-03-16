using System;
using Microsoft.Extensions.Logging;

namespace AppViewLite
{
    internal class ConsoleLogger : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull
        {
            return null;
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return true;
        }

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            LoggableBase.Log(eventId + ": " + formatter(state, exception));
        }
    }
}



