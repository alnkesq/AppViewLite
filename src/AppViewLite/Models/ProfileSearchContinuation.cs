namespace AppViewLite.Models
{
    internal record struct ProfileSearchContinuation(Plc MaxPlc, bool AlsoSearchDescriptions)
    {
        public readonly string Serialize() => MaxPlc.PlcValue + "_" + (AlsoSearchDescriptions ? 1 : 0);
        public static ProfileSearchContinuation Deserialize(string s)
        {
            var parts = s.Split('_');
            return new ProfileSearchContinuation(new Plc(int.Parse(parts[0])), parts[1] == "1");
        }
    }
}

