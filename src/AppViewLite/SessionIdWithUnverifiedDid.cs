using System;

namespace AppViewLite
{
    public record struct SessionIdWithUnverifiedDid(string UnverifiedDid, string SecretKey)
    {
        public override string ToString()
        {
            return SecretKey + "=" + UnverifiedDid;
        }
        public static SessionIdWithUnverifiedDid Parse(string s)
        {
            var eq = s.IndexOf('=');
            if (eq == -1) throw new ArgumentException();
            return new(s.Substring(eq + 1), s.Substring(0, eq));
        }
    }
}



