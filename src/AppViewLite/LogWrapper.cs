using Microsoft.Extensions.Logging;
using System;

namespace AppViewLite
{
    public class LogWrapper : LoggableBase, ILogger
    {
        private readonly LogLevel exceptionMinLogLevel;
        private readonly LogLevel messageMinLogLevel;

        public LogWrapper()
            : this(LogLevel.Warning, LogLevel.Warning)
        {

        }
        public LogWrapper(LogLevel exceptionMinLogLevel, LogLevel messageMinLogLevel)
        {
            this.exceptionMinLogLevel = exceptionMinLogLevel;
            this.messageMinLogLevel = messageMinLogLevel;
        }
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull
        {
            return null;
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return logLevel >= exceptionMinLogLevel || logLevel >= messageMinLogLevel;
        }

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            var text = logLevel + ": " + formatter(state, exception);
            if (exception != null)
            {
                if (logLevel >= exceptionMinLogLevel)
                {
                    LogNonCriticalException(text, exception);
                }
                else
                {
                    LogLowImportanceException(text, exception);
                }
            }
            else
            {
                if (logLevel >= messageMinLogLevel && !IsLowImportanceMessage(text))
                {
                    Log(text);
                }
                else
                {
                    LogInfo(text);
                }
            }
        }

        public class Provider : ILoggerProvider
        {
            public ILogger CreateLogger(string categoryName)
            {
                return new LogWrapper();
            }

            public void Dispose()
            {
                LoggableBase.FlushLog();
            }
        }
    }
}
