using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public record struct BlockReason(BlockReasonKind Kind, Relationship List)
    {

        public string? ToDisplayString()
        {
            if (this == default) return null;
            if (List != default)
            {
                return Kind switch
                {
                    BlockReasonKind.BlockedBy => "You are blocked, because of a blocklist you're in.",
                    BlockReasonKind.Blocks => "You are subscribed to a blocklist that includes this user.",
                    BlockReasonKind.MutualBlock => "You block each other.",
                    _ => throw new Exception()
                };
            }
            else
            {
                return Kind switch
                {
                    BlockReasonKind.BlockedBy => "You are blocked.",
                    BlockReasonKind.Blocks => "You block this user.",
                    BlockReasonKind.MutualBlock => "You block each other.",
                    _ => throw new Exception()
                };

            }
        }
    }

    [Flags]
    public enum BlockReasonKind
    { 
        None = 0,
        BlockedBy = 1,
        Blocks = 2,
        MutualBlock = BlockedBy | Blocks
    }
}

