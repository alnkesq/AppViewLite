using AppViewLite.Numerics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public record struct TopPostSearchCursor(int MinLikes, DuckDbUuid SearchId, int PageIndex)
    {

        public string Serialize() => PageIndex + "-" + MinLikes + "-" + SearchId.ToString().Replace("-", null);
        public static TopPostSearchCursor Deserialize(string s)
        {
            var array = s.Split('-');
            return new TopPostSearchCursor(int.Parse(array[1]), DuckDbUuid.Parse(array[2]), int.Parse(array[0]));
        }
    }
}

