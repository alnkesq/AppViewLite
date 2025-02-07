using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public enum ModerationBehavior
    {
        None,
        Mute,
        Block,
        BlurAll,
        BlurImageOnly,
        Badge,
    }
}

