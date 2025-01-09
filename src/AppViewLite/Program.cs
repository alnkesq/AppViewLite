using System;
using System.Threading.Tasks;

namespace AppViewLite
{


    public class AppViewProgram
    {

        static async Task Main(string[] args)
        {
            using var relationships = new BlueskyRelationships();

            Console.CancelKeyPress += (s, e) =>
            {
                lock (relationships)
                {
                    Console.Error.WriteLine("Flushing...");
                    relationships.Dispose();
                    Console.Error.WriteLine("Exiting.");
                    Environment.Exit(0);
                }
            };

            var indexer = new Indexer(relationships);
            await indexer.ListenJetStreamFirehoseAsync();
        }

        



    }

}

