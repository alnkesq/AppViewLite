using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    public static class ExtensionMethods
    {
        public static void FireAndForget(this Task task)
        {
            task.ContinueWith(t =>
            {
                if (t.IsFaulted)
                {
                    Console.Error.WriteLine(t.Exception);
                }
            }, TaskContinuationOptions.OnlyOnFaulted);
        }
    }
}

