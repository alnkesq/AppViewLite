using DuckDbSharp.Types;

namespace AppViewLite.Models
{
    public record struct TopPostSearchCursor(int MinLikes, DuckDbUuid SearchId, int PageIndex)
    {

        public readonly string Serialize() => PageIndex + "-" + MinLikes + "-" + SearchId.ToString().Replace("-", null);
        public static TopPostSearchCursor Deserialize(string s)
        {
            var array = s.Split('-');
            return new TopPostSearchCursor(int.Parse(array[1]), DuckDbUuid.Parse(array[2]), int.Parse(array[0]));
        }
    }
}

