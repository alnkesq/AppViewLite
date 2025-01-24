using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public record struct PostStatsNotification(long LikeCount, long RepostCount, long QuoteCount, long ReplyCount)
    {
    }
}

