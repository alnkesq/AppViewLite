namespace AppViewLite.Models;

using AppViewLite.Numerics;

public record struct CollectionAndRKey(string Collection, string RKey)
{
    public CollectionAndRKey(string collection, Tid rkey) : this(collection, rkey.ToString()!)
    { 
    }

    public static CollectionAndRKey ParseUnprefixed(string path)
    {
        var slash = path.IndexOf('/');
        return new CollectionAndRKey(path.Substring(0, slash), path.Substring(slash + 1));
    }

    public override string ToString()
    {
        return Collection + "/" + RKey;
    }
}
