using AppViewLite.Models;
using System;
using System.Collections.Concurrent;

namespace AppViewLite
{
    public class AppViewLiteSession
    {



        public Guid Id = Guid.NewGuid();

        public DateTime LastSeen;

        public string? LoggedInUserString => Profile?.Did;
        public Plc? LoggedInUser;

        public bool IsLoggedIn => LoggedInUser != null;
        public BlueskyProfile? Profile;

    }

}

