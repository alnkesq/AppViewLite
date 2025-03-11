using FishyFlip.Models;
using AppViewLite.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Threading.Tasks;
using System.IdentityModel.Tokens.Jwt;

namespace AppViewLite
{
    public class AppViewLiteSession
    {
        // Threading: same as AppViewLiteUserContext
        public DateTime LastSeen;
        public bool IsReadOnlySimulation;
        public DateTime LoginDate;
        public required string? SessionToken;
        public string? LoggedInUserString => Profile?.Did;
        public Plc? LoggedInUser => UserContext.LoggedInUser;

        public bool IsLoggedIn => LoggedInUser != null;
        public string? Did => UserContext.Did;
        public BlueskyProfile? Profile => UserContext.Profile;

        public required AppViewLiteUserContext UserContext; // Multiple sessions ("cookies") can share the same user context if they refer to the same user.

        public static AppViewLiteSession CreateAnonymous() => new AppViewLiteSession { SessionToken = null, UserContext = new() };

    }

    public class AppViewLiteUserContext
    {
        // Threading: can be accessed concurrently without locks.
        // Collections can be only replaced, never updated in place.
        // However, writers can choose to lock for their own self consistency.


        public BlueskyProfile? Profile;
        public string? Did => Profile?.Did;

        public Session? PdsSession;

        public Dictionary<Plc, PrivateFollow> PrivateFollows = new();
        public AppViewLiteProfileProto? PrivateProfile;
        public IEnumerable<ListEntry> PrivateFollowsAsListEntries => PrivateFollows.Keys.Select(x => new ListEntry(x, default));
        public Plc? LoggedInUser => Profile?.Plc;
        public PrivateFollow GetPrivateFollow(Plc plc)
        {
            return PrivateFollows.TryGetValue(plc, out var f) ? f : new PrivateFollow { Plc = plc.PlcValue };
        }

        public Task? InitializeAsync;

        public long MinVersion;

        public AppViewLiteSessionProto? TryGetAppViewLiteSession(string sessionId)
        {
            return PrivateProfile!.Sessions.FirstOrDefault(x => CryptographicOperations.FixedTimeEquals(MemoryMarshal.AsBytes<char>(x.SessionToken), MemoryMarshal.AsBytes<char>(sessionId)));
        }

        public DateTime? RefreshTokenExpireDate;
        public void UpdateRefreshTokenExpireDate()
        {
            var refreshJwt = PdsSession?.RefreshJwt;
            if (refreshJwt == null)
            {
                RefreshTokenExpireDate = null;
                return;
            }

            var handler = new JwtSecurityTokenHandler();
            var jwtToken = handler.ReadJwtToken(refreshJwt);
            RefreshTokenExpireDate = jwtToken.ValidTo;
        }


    }

}

