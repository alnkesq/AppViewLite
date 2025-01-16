using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public class PostSearchOptions
    {
        public required string? Query;
        public int MinLikes;
        public int MinReposts;
        public DateTime? Since;
        public DateTime? Until;
        public string? Author;
    }
}

