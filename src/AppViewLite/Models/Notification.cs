using FishyFlip.Lexicon.Com.Shinolabs.Pinksea;
using AppViewLite.Numerics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public record struct Notification(ApproximateDateTime32 EventDate, Plc Actor, Tid RKey, NotificationKind Kind) : IComparable<Notification>
    {
        // Semantics of RKey are kind-dependant.

        public int CompareTo(Notification other)
        {
            var cmp = this.EventDate.CompareTo(other.EventDate);
            if (cmp != 0) return cmp;
            cmp = this.Actor.CompareTo(other.Actor);
            if (cmp != 0) return cmp;
            cmp = this.RKey.CompareTo(other.RKey);
            if (cmp != 0) return cmp;
            cmp = this.Kind.CompareTo(other.Kind);
            return cmp;
        }

        public string Serialize() => string.Join("/", EventDate.Value, Actor.PlcValue, RKey.TidValue, (int)Kind);
        public static Notification Deserialize(string s)
        {
            var parts = s.Split('/');
            return new Notification(new(uint.Parse(parts[0])), new(int.Parse(parts[1])), new(long.Parse(parts[2])), (NotificationKind)int.Parse(parts[3]));
        }
    }

    public enum NotificationKind : byte
    { 
        None,
        FollowedYou,
        LikedYourPost,
        RepostedYourPost,
        QuotedYourPost,
        MentionedYou,
        RepliedToYourPost,
        RepliedtoYourThread,
    }
}

