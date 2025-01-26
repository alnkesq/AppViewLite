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

            var indexer = new Indexer(relationships, AtProtocolProvider.CreateDefault());
            Console.Error.WriteLine("Indexing the firehose to " + relationships.BaseDirectory + "...");
            Console.Error.WriteLine("NOTE: If you want to use the Web UI, run AppViewLite.Web instead.");
            Console.Error.WriteLine("Press CTRL+C to stop indexing...");
            await indexer.ListenJetStreamFirehoseAsync();
        }

        



    }

}

