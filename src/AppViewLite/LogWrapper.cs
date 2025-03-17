using Microsoft.Extensions.Logging;
using System;

namespace AppViewLite
{
    public class LogWrapper : LoggableBase, ILogger
    {
        private readonly LogLevel exceptionMinLogLevel;
        private readonly LogLevel messageMinLogLevel;
        public Func<Exception, bool>? IsLowImportanceException;
        public new Func<string, bool>? IsLowImportanceMessage;
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
                if (logLevel >= exceptionMinLogLevel && IsLowImportanceException?.Invoke(exception) != true && !(IsLowImportanceMessage?.Invoke(text)).GetValueOrDefault())
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
                if (logLevel >= messageMinLogLevel && !(LoggableBase.IsLowImportanceMessage(text) || (IsLowImportanceMessage?.Invoke(text)).GetValueOrDefault()))
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
