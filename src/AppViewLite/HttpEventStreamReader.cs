using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AppViewLite
{
    public class HttpEventStreamReader : IDisposable
    {
        public StreamReader BaseReader { get; private set; }
        public HttpEventStreamReader(Stream response)
        {
            BaseReader = new StreamReader(response);
        }

        public void Dispose()
        {
            BaseReader.Dispose();
        }

        public async Task<HttpEventStreamEvent?> ReadAsync(CancellationToken ct = default)
        {
            var evt = new HttpEventStreamEvent();
            while (true)
            {
                var line = await BaseReader.ReadLineAsync(ct);
                if (line == null) return null;

                if (line.Length == 0)
                {
                    return evt;
                }
                var colon = line.IndexOf(':');
                if (colon == -1) throw new InvalidDataException("Missing colon in HTTP event stream");
                var t = line.Substring(0, colon);
                if (line[colon + 1] != ' ') throw new InvalidDataException("Missing space after colon in HTTP event stream.");
                var v = line.Substring(colon + 2);
                if (t == "event") evt.Event = v;
                else if (t == "data") evt.Data = v;
                else if (t == "id") evt.Id = v;
                else if (t == "retry") evt.Retry = int.Parse(v);
            }
        }
    }

    public class HttpEventStreamEvent
    {
        public string? Event;
        public string? Id;
        public string? Data;
        public int? Retry;
    }
}

