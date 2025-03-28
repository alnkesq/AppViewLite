using AppViewLite;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;

namespace AppViewLite.Storage
{
    public static class ProbabilisticSetIo
    {

        public static void WriteCompressedProbabilisticSetToFile<T>(string destination, ProbabilisticSet<T> probabilisticSet) where T : unmanaged
        {
            using (var stream = new System.IO.Compression.GZipStream(new FileStream(destination + ".tmp", FileMode.Create, FileAccess.Write, FileShare.None), System.IO.Compression.CompressionLevel.Fastest))
            {
                stream.Write(probabilisticSet.ArrayAsBytes);
            }
            File.Move(destination + ".tmp", destination, true);
        }

        public static IEnumerable<ReadOnlyMemory<ulong>> ReadCompressedProbabilisticSetFromFile(string path)
        {
            using var stream = new System.IO.Compression.GZipStream(new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read), System.IO.Compression.CompressionMode.Decompress);
            var buffer = new ulong[8 * 1024];
            while (true)
            {

                var bufferAsBytes = MemoryMarshal.AsBytes<ulong>(buffer);
                var readBytes = stream.ReadAtLeast(bufferAsBytes, bufferAsBytes.Length, throwOnEndOfStream: false);
                if (readBytes == 0) yield break;
                yield return new ReadOnlyMemory<ulong>(buffer, 0, readBytes / 8);
            }
        }
    }

}
