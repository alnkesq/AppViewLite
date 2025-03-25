using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;

namespace AppViewLite
{
    public static class MemoryMappedFileUtils
    {
        // timeout "${SECONDS}s" bpftrace -e 'tracepoint:exceptions:page_fault_user /pid == '$PID'/ { printf("%lx\n", args->address); }' > faults.txt
        // cp /proc/$PID/maps mmaps.txt
        public static void PageFaultsToSlices(string pageFaultTrace, string mmaps, Func<string, string?> pathToRelativePath)
        {
            var ranges = ParseProcMmap(mmaps);
            var sliceSetToCount = new Dictionary<(string Table, string Kind), int>();
            foreach (var perfLine in File.ReadLines(pageFaultTrace))
            {
                if (perfLine.StartsWith("Attaching", StringComparison.Ordinal)) continue;
                if (string.IsNullOrEmpty(perfLine)) continue;
                //var parts = perfLine.Split(':', StringSplitOptions.TrimEntries);
                var addr = ulong.Parse(perfLine, System.Globalization.NumberStyles.HexNumber);

                MemoryRange range = default;
                var index = ranges.AsSpan().BinarySearch(new MemoryRange(addr, addr, null));
                if (index < 0)
                {
                    index = ~index;
                    if (index != 0)
                    {
                        range = ranges[index - 1];
                        if (addr < range.Start || addr >= range.End)
                            range = default;
                    }
                }
                else
                {
                    range = ranges[index];
                }
                //if (range.Path == "/memfd:doublemapper (deleted)") continue;

                if (range != default)
                {
                    var length = range.End - range.Start;
                    var ratio = (double)(addr - range.Start) / length;

                    string? displayText = range.Path;
                    var f = range.Path != null ? pathToRelativePath(range.Path) : null;
                    if (f != null)
                    {
                        var table = Path.GetDirectoryName(f)!;
                        var kind = f switch
                        {
                            _ when f.EndsWith(".col0.dat", StringComparison.Ordinal) => "KEYS",
                            _ when f.EndsWith(".col1.dat", StringComparison.Ordinal) => "VALUES",
                            _ when f.EndsWith(".col2.dat", StringComparison.Ordinal) => "OFFSETS",
                            _ when f.EndsWith(".cache", StringComparison.Ordinal) => "CACHE",
                            _ => "UNKNOWN",
                        };
                        displayText = table + "_" + kind + " of " + Path.GetFileName(f).Split('.')[0];
                        CollectionsMarshal.GetValueRefOrAddDefault(sliceSetToCount, (table, kind), out _)++;
                    }

              

                    //Console.Error.WriteLine(addr.ToString("x") + "  " + (ratio * 100).ToString("0.0") + "%" + "  " + StringUtils.ToHumanBytes((long)length) + "\t\t" + displayText);
                }
            }
            foreach (var item in sliceSetToCount.OrderByDescending(x => x.Value).Take(20))
            {
                Console.Error.WriteLine($"{item.Key.Table,50} {item.Key.Kind,-10} {item.Value,-5}");
            }
        }

        private static MemoryRange[] ParseProcMmap(string mmaps)
        {
            var result = new List<MemoryRange>();
            foreach (var line in File.ReadLines(mmaps))
            {
                var fields = line.Split(' ', 6);
                var range = fields[0].Split('-');
                result.Add(new MemoryRange(ulong.Parse(range[0], System.Globalization.NumberStyles.HexNumber), ulong.Parse(range[1], System.Globalization.NumberStyles.HexNumber), fields[5].TrimStart()));
            }
            return result.Order().ToArray();
        }

        record struct MemoryRange(ulong Start, ulong End, string? Path) : IComparable<MemoryRange>
        {
            public int CompareTo(MemoryRange other)
            {
                var cmp = this.Start.CompareTo(other.Start);
                if (cmp != 0) return cmp;
                cmp = this.End.CompareTo(other.End);
                if (cmp != 0) return cmp;
                return StringComparer.Ordinal.Compare(this.Path, other.Path);
            }

            public override string ToString()
            {
                return Start.ToString("x") + "-" + End.ToString("x") + (Path != null ? " " + Path : null);
            }
        }
    }
}

