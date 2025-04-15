using AppViewLite.Storage;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Net.Sockets;
using System.Net.WebSockets;

namespace AppViewLite
{
    public abstract class LoggableBase
    {
        public static StreamWriter? LogFile;
        public static string? LogDirectory;
        public static void Initialize()
        {
            if (LogFile != null) return;
            LogDirectory = Path.Combine(AppViewLiteConfiguration.GetDataDirectory(), "logs");
            Directory.CreateDirectory(LogDirectory);

            CombinedPersistentMultiDictionary.LogCallback = Log;

            LogFile = new StreamWriter(Path.Combine(LogDirectory, DateTime.UtcNow.ToString("yyyy-MM-dd-HH-mm-ss") + ".log"), new FileStreamOptions
            {
                Mode = FileMode.Append,
                Access = FileAccess.Write,
                Share = FileShare.Read | FileShare.Delete,
            });
            LogFile.AutoFlush = true;
        }


        private static void LogToFile(string text)
        {
            if (LogFile != null)
            {
                lock (LogFile)
                {
                    LogFile.WriteLine($"[{DateTime.UtcNow.ToString("o")}] {text}");
                }
            }
        }

        public static void Log(string text)
        {
            Console.Error.WriteLine(text);
            LogToFile(text);
        }


        private static bool IsLowImportanceException(Exception ex)
        {
            return ex.AnyInnerException(IsLowImportanceExceptionCore);
        }

        private static bool IsLowImportanceExceptionCore(Exception ex)
        {
            if (ex is UnexpectedFirehoseDataException) return true;

            return false;
        }

        public static void LogNonCriticalException(Exception ex)
        {
            if (IsAssertionLite(ex)) return; // Already logged at throw time
            if (IsLowImportanceException(ex))
                LogInfo(ex.Message);
            else
                Log(ExceptionToString(ex));
        }

        private static string ExceptionToString(Exception ex)
        {
            var inner = ex;
            var messages = new List<string>();
            while (inner != null)
            {
                messages.Add(inner.Message);

                var concise = false;
                if (inner is HttpRequestException http)
                {
                    if (http.StatusCode != null)
                    {
                        messages.Add("HTTP " + (int)http.StatusCode + " (" + http.StatusCode + ")");
                        concise = true;
                    }
                    else
                    {
                        concise = true;
                    }
                }

                if (inner is WebSocketException ws)
                {
                    concise = true;
                }

                if (inner is SocketException so)
                {
                    concise = true;
                }

                if (concise)
                {
                    return string.Join(": ", messages);
                }

                inner = inner.InnerException;
            }
            return ex.ToString();
        }

        private static bool IsAssertionLite(Exception ex)
        {
            return ex.AnyInnerException(x => x is AssertionLiteException);
        }

        public static void LogNonCriticalException(string text, Exception ex)
        {
            if (IsAssertionLite(ex)) return; // Already logged at throw time

            if (IsLowImportanceException(ex) || IsLowImportanceMessage(text))
                LogInfo(text + ": " + ex.Message);
            else
                Log(text + ": " + ExceptionToString(ex));
        }

        public static bool IsLowImportanceMessage(string text)
        {
            return
                text.Contains("Failed to deserialize ATWebSocketRecord", StringComparison.Ordinal) ||
                text.Contains(@"""kind"":""commit"",""commit"":{""rev"":""") ||
                text.EndsWith("}}", StringComparison.Ordinal); // JSON dumps from FishyFlip
        }

        public static void LogLowImportanceException(Exception ex)
        {
            if (IsAssertionLite(ex)) return; // Already logged at throw time
            LogInfo(ex.Message);
        }
        public static void LogLowImportanceException(string text, Exception ex)
        {
            if (IsAssertionLite(ex)) return; // Already logged at throw time
            LogInfo(text + ": " + ex.Message);
        }

        public static void LogInfo(string text)
        {
            Console.Error.WriteLine(text);
        }

        public static void FlushLog()
        {
            if (LogFile != null)
            {
                lock (LogFile)
                {
                    LogFile.Flush();
                }
            }
        }
    }


}

