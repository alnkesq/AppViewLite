using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    [Flags]
    internal enum PostDataEncoding : byte
    {
        BpeOnly = 0,

        InReplyToRKey = 1,
        InReplyToPlc = 2,
        RootPostRKey = 4,
        RootPostPlc = 8,

        Language = 16,


        Proto = RootPostPlc, // PLC should imply RKey, so this is an invalid combination, that we use to represent Proto and save 1 bit (we only have 8, we must save them)
        //BrotliProto = InReplyToPlc, 

    }
}

