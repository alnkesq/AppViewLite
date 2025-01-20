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
    }

}

