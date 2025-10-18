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
    public class FirehoseCursor
    {
        [ProtoMember(1)][JsonInclude] public required string FirehoseUrl;
        [ProtoMember(2)][JsonInclude] public string? CommittedCursor;
        [ProtoMember(3)][JsonInclude] public DateTime CursorCommitDate;

        [ProtoMember(4)][JsonInclude] public DateTime FirehoseTimeLastProcessed; // NOT for firehose checkpointing, only for display/debug purposes.
        [JsonInclude] public DateTime SystemTimeLastProcessed;
        [JsonInclude] public DateTime SystemTimeLastRawReceived;

        [JsonInclude][JsonConverter(typeof(JsonStringEnumConverter))] public FirehoseState State;
        [JsonIgnore] public Exception? LastException;
        [JsonInclude] public DateTime LastExceptionDate;
        [JsonInclude] public string? LastError => LastException?.Message;
        [JsonInclude] public TimeSpan? LagBehind => FirehoseTimeLastProcessed != default ? DateTime.UtcNow - FirehoseTimeLastProcessed : null;
        [JsonInclude] public long ReceivedEvents;
        [JsonInclude] public long RestartCount;

        internal void MakeUtc()
        {
            ExtensionMethods.MakeUtc(ref CursorCommitDate);
            ExtensionMethods.MakeUtc(ref FirehoseTimeLastProcessed);
            ExtensionMethods.MakeUtc(ref SystemTimeLastProcessed);
            ExtensionMethods.MakeUtc(ref SystemTimeLastRawReceived);
        }
    }

    public enum FirehoseState
    {
        None,
        Starting,
        Running,
        Error,
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

