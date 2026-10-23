using Microsoft.Extensions.Logging;
using System;
using System.Diagnostics.CodeAnalysis;

namespace AppViewLite
{
    public class LogWrapper : LoggableBase, ILogger
    {
        private readonly LogLevel exceptionMinLogLevel;
        private readonly LogLevel messageMinLogLevel;
        private readonly LogLevel messageStdErrMinLogLevel;
        public Func<Exception, bool>? IsLowImportanceException;
        public new Func<string, bool>? IsLowImportanceMessage;
        public readonly string? Category;
        public LogWrapper(string? category = null)
            : this(category, LogLevel.Warning, LogLevel.Warning, LogLevel.Debug)
        {

        }
        public LogWrapper(string? category, LogLevel exceptionMinLogLevel, LogLevel messageMinLogLevel, LogLevel messageStdErrMinLogLevel)
        {
            if (messageStdErrMinLogLevel > messageMinLogLevel) AssertionLiteException.Throw("messageStdErrMinLogLevel is more than messageMinLogLevel");
            this.exceptionMinLogLevel = exceptionMinLogLevel;
            this.messageMinLogLevel = messageMinLogLevel;
            this.messageStdErrMinLogLevel = messageStdErrMinLogLevel;
            this.Category = category;
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
            if (exception == null && logLevel < messageStdErrMinLogLevel) return;
            var text = logLevel + ": " + (Category != null ? "[" + Category + "] " : null) + formatter(state, exception);
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
                return new LogWrapper(categoryName);
            }

            public void Dispose()
            {
                LoggableBase.FlushLog();
            }
        }
        public class LogFactory : ILoggerFactory
        {
            private readonly LogLevel exceptionMinLogLevel;
            private readonly LogLevel messageMinLogLevel;
            private readonly LogLevel messageStdErrMinLogLevel;
            public Func<Exception, bool>? IsLowImportanceException;
            public new Func<string, bool>? IsLowImportanceMessage;

            public LogFactory()
                : this(LogLevel.Warning, LogLevel.Warning, LogLevel.Debug)
            {

            }

            public LogFactory(LogLevel exceptionMinLogLevel, LogLevel messageMinLogLevel, LogLevel messageStdErrMinLogLevel)
            {
                this.exceptionMinLogLevel = exceptionMinLogLevel;
                this.messageMinLogLevel = messageMinLogLevel;
                this.messageStdErrMinLogLevel = messageStdErrMinLogLevel;
            }
            public void AddProvider(ILoggerProvider provider)
            {
            }

            public ILogger CreateLogger(string categoryName)
            {
                return new LogWrapper(categoryName, exceptionMinLogLevel, messageMinLogLevel, messageStdErrMinLogLevel)
                {
                    IsLowImportanceException = IsLowImportanceException,
                    IsLowImportanceMessage = IsLowImportanceMessage,
                };
            }

            public void Dispose()
            {
            }
        }
    }
}
