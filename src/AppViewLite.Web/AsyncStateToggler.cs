using AppViewLite.Models;
using AppViewLite.Numerics;
using System;

namespace AppViewLite.Web
{
    public class AsyncStateToggler
    {
        public long ActorCount { get; set; }
        private Tid? rkey;
        public bool HaveRelationship { get; set; }
        private bool busy;
        private Func<Task<Tid>> addRelationship;
        private Func<Tid, Task> deleteRelationship;
        private Action notifyChange;
        public string? RKey => rkey?.ToString();

        public AsyncStateToggler(long actorCount, Tid? rkey, Func<Task<Tid>> addRelationship, Func<Tid, Task> deleteRelationship, Action notifyChange)
        {
            this.ActorCount = actorCount;
            this.rkey = rkey;
            this.HaveRelationship = rkey != null;
            this.notifyChange = notifyChange;
            this.addRelationship = addRelationship;
            this.deleteRelationship = deleteRelationship;
        }

        public async Task ToggleIfNotBusyAsync()
        {
            if (busy) return;
            busy = true;
            var prevState = (HaveRelationship, rkey, ActorCount);
            try
            {
                

                HaveRelationship = !HaveRelationship;


                if (HaveRelationship)
                {
                    ActorCount++;
                    notifyChange();
                    rkey = await addRelationship();
                }
                else
                {
                    if(ActorCount > 0)
                        ActorCount--;
                    notifyChange();
                    await deleteRelationship(rkey!.Value);
                }


                notifyChange();

            }
            catch(Exception ex)
            {
                (HaveRelationship, rkey, ActorCount) = prevState;
                notifyChange();
                Console.Error.WriteLine("Toggle failed: " + ex);
            }
            finally
            {
                busy = false;
            }
            
        }


    }
}

