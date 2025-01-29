using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    [ProtoContract]
    public class RepositoryImportEntry
    {
        [ProtoMember(1)] public RepositoryImportKind Kind;
        [ProtoMember(2)] public long DurationMillis;
        [ProtoMember(3)] public long LastRevOrTid;
        [ProtoMember(4)] public string? Error;

        public DateTime StartDate;
        public Plc Plc;

    }


    public enum RepositoryImportKind
    { 
        None,
        Full,
        Posts,
        Likes,
        Follows,
        Reposts,
        Blocks,
        ListMetadata,
        ListEntries,
        BlocklistSubscriptions,
        FeedGenerators,
        Threadgates,
        Postgates,
    }

}

