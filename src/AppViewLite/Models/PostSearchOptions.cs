using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public record PostSearchOptions
    {
        public required string? Query { get; init; }
        public int MinLikes { get; init; }
        public int MinReposts { get; init; }
        public DateTime? Since { get; init; }
        public DateTime? Until { get; init; }
        public string? Author { get; init; }
        public LanguageEnum Language { get; init; }
    }
}

