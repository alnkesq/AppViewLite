namespace AppViewLite.Models
{
    public record struct RelationshipStr(string Did, string RKey)
    {
        public override readonly string ToString()
        {
            return Did + "/" + RKey;
        }
    }
}



