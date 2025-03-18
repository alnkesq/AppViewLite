using ProtoBuf;
using AppViewLite.Storage;
using System;
using System.Collections.Generic;

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
        [ProtoMember(3)] public long PruneId;
        public override string ToString()
        {
            return SliceBaseName;
        }

        public string SliceBaseName => ToSliceName().BaseName;

        public SliceName ToSliceName()
        {
            return new SliceName(new DateTime(StartTime, DateTimeKind.Utc), new DateTime(EndTime, DateTimeKind.Utc), PruneId);
        }
    }
}

