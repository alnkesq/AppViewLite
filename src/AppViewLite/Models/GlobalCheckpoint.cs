using ProtoBuf;
using AppViewLite.Storage;
using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace AppViewLite.Models
{
    [ProtoContract]
    internal class GlobalCheckpoint
    {
        [ProtoMember(1)] public List<GlobalCheckpointTable>? Tables;
        [ProtoMember(2)] public bool Dummy;

        [ProtoMember(3)] public List<FirehoseCursor>? FirehoseCursors;
    }

    [ProtoContract]
    internal class FirehoseCursor
    {
        [ProtoMember(1)][JsonInclude] public required string FirehoseUrl;
        [ProtoMember(2)][JsonInclude] public string? CommittedCursor;
        [ProtoMember(3)][JsonInclude] public DateTime CursorCommitDate;

        [ProtoMember(4)][JsonInclude] public DateTime LastSeenEventDate; // not necessarily processed yet, only for display/debug purposes.

        public TimeSpan? LagBehind => LastSeenEventDate != default ? DateTime.UtcNow - LastSeenEventDate : null;

        internal void MakeUtc()
        {
            ExtensionMethods.MakeUtc(ref CursorCommitDate);
            ExtensionMethods.MakeUtc(ref LastSeenEventDate);
        }
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

