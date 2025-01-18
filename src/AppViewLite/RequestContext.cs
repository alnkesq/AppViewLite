using AppViewLite;
using AppViewLite.Models;
using System;
using System.Threading.Tasks;
namespace AppViewLite
{
    public record RequestContext(Task? DeadlineReached, AppViewLiteSession? Session)
    {
        public static RequestContext Create(AppViewLiteSession? session = null)
        {
            return new RequestContext(Task.Delay(1500), session);
        }

        public static RequestContext CreateInfinite(AppViewLiteSession? session)
        {
            return new RequestContext(null, session);
        }

        public bool IsLoggedIn => Session != null && Session.IsLoggedIn;
        public Plc LoggedInUser => Session!.LoggedInUser!.Value;
    }

}

