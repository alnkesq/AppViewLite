using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public record struct BlockReason(BlockReasonKind Kind, Relationship List);

    [Flags]
    public enum BlockReasonKind
    { 
        None = 0,
        BlockedBy = 1,
        Blocks = 2,
        MutualBlock = BlockedBy | Blocks
    }
}

