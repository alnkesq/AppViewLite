using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ZstdSharp.Unsafe;

namespace AppViewLite.Models
{
    [ProtoContract]
    internal class GlobalCheckpoint
    {
        [ProtoMember(1)] public List<GlobalCheckpointTable>? Tables;
        [ProtoMember(2)] public bool Dummy;
    }

    [ProtoContract]
    internal class GlobalCheckpointTable
    {
        [ProtoMember(1)] public required string Name;
        [ProtoMember(2)] public GlobalCheckpointSlice[]? Slices;

        public override string ToString()
        {
            return Name;
        }

    }

    [ProtoContract]
    internal class GlobalCheckpointSlice
    {
        [ProtoMember(1)] public long StartTime;
        [ProtoMember(2)] public long EndTime;
        public override string ToString()
        {
            return StartTime + "-" + EndTime;
        }
    }
}

