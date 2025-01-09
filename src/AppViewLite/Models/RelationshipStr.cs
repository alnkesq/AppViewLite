namespace AppViewLite.Models
{
    public record struct RelationshipStr(string Did, string RKey)
    {
        public override string ToString()
        {
            return Did + "/" + RKey;
        }
    }
}



