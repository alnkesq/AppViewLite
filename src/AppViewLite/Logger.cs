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

        public static void Initialize()
        {
            if (LogFile != null) return;
            var directory = Path.Combine(AppViewLiteConfiguration.GetDataDirectory(), "logs");
            Directory.CreateDirectory(directory);
            LogFile = new StreamWriter(Path.Combine(directory, DateTime.UtcNow.ToString("yyyy-MM-dd-HH-mm-ss") + ".log"), true);
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
        public static void LogNonCriticalException(Exception ex)
        {
            Log(ex.ToString());
        }

        public static void LogNonCriticalException(string text, Exception ex)
        {
            Log(text + ": " + ex.ToString());
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

