using FishyFlip.Models;
using AppViewLite.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace AppViewLite
{
    public class AppViewLiteSession
    {



        public Guid Id = Guid.NewGuid();

        public DateTime LastSeen;
        public required bool IsReadOnlySimulation;

        public string? LoggedInUserString => Profile?.Did;
        public Plc? LoggedInUser;

        public bool IsLoggedIn => LoggedInUser != null;
        public BlueskyProfile? Profile;
        public string? Did => Profile?.Did;

        public Session? PdsSession;

        public HashSet<LabelId> NeedLabels;
    }

}

