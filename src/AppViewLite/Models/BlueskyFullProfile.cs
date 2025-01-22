using System.Collections.Generic;

namespace AppViewLite.Models
{
    public class BlueskyFullProfile
    {
        public BlueskyProfile Profile;
        public long Following;
        public long Followers;
        public bool FollowsYou;
        public bool YouAreFollowing;
        public List<BlueskyProfile>? FollowedByPeopleYouFollow;
    }
}

