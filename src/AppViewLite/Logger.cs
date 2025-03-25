using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
            if (IsLowImportanceException(ex))
                LogInfo(ex.Message);
            else
                Log(ex.ToString());
        }

        public static void LogNonCriticalException(string text, Exception ex)
        {
            if (IsLowImportanceException(ex) || IsLowImportanceMessage(text))
                LogInfo(text + ": " + ex.Message);
            else
                Log(text + ": " + ex.ToString());
        }

        public static bool IsLowImportanceMessage(string text)
        {
            return
                text.Contains("Failed to deserialize ATWebSocketRecord", StringComparison.Ordinal) ||
                text.Contains(@"""kind"":""commit"",""commit"":{""rev"":""");
        }

        public static void LogLowImportanceException(Exception ex)
        {
            LogInfo(ex.Message);
        }
        public static void LogLowImportanceException(string text, Exception ex)
        {
            LogInfo(text + ": " + ex.Message);
        }

        internal static void LogInfo(string text)
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

