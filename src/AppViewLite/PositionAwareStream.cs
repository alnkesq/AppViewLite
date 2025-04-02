using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AppViewLite
{
    public class PositionAwareStream : Stream
    {
        private Stream stream;
        private long position;
        public PositionAwareStream(Stream stream)
        {
            this.stream = stream;
        }
        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => throw new NotSupportedException("PositionAwareStream does not support get_Length");

        public override long Position { get => position; set => throw new NotSupportedException("PositionAwareStream does not support set_Position"); }

        public override void Flush()
        {
            throw new NotSupportedException("PositionAwareStream does not support Flush.");
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var read = stream.Read(buffer, offset, count);
            position += read;
            return read;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException("PositionAwareStream does not support Seek.");
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException("PositionAwareStream does not support SetLength.");
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException("PositionAwareStream does not support Write.");
        }

        public override int Read(Span<byte> buffer)
        {
            var read = stream.Read(buffer);
            position += read;
            return read;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            var read = await stream.ReadAsync(buffer, cancellationToken);
            position += read;
            return read;
        }
        protected override void Dispose(bool disposing)
        {
            if (disposing)
                stream.Dispose();
        }
        public override ValueTask DisposeAsync()
        {
            return stream.DisposeAsync();
        }
    }
}

