using System;
using System.IO;

namespace AppViewLite.Storage
{
    public class ReloadableFile<T>
    {
        private readonly string? path;
        private readonly Func<string?, T> read;
        private DateTime lastChecked;
        private DateTime loadedLastWriteTime;
        private long loadedSize;
        private T? value;
        public ReloadableFile(string? path, Func<string?, T> read)
        {
            this.path = path;
            this.read = read;
        }

        private TimeSpan CheckInterval = TimeSpan.FromSeconds(2);
        public T GetValue()
        {
            if (path == null)
            {
                if (value == null)
                {
                    lock (this)
                    {
                        if (value == null)
                        {
                            value = read(null);
                        }
                    }
                }
                return value;
            }



            var now = DateTime.UtcNow;
            if (value != null && (now - lastChecked) < CheckInterval)
            {
                return value;
            }

            lock(this)
            {
                if (value == null || (now - lastChecked) >= CheckInterval)
                {
                    var date = File.GetLastWriteTimeUtc(path);
                    var size = new FileInfo(path).Length;
                    if (value == null || date != loadedLastWriteTime || size != loadedSize)
                    {

                        value = read(path);
                        loadedLastWriteTime = date;
                        loadedSize = size;
                    }
                    lastChecked = DateTime.UtcNow;
                }
                return value!;
            }
        }

    }
}

