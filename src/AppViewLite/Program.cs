using AppViewLite.Models;
using AppViewLite.Storage;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace AppViewLite
{


    public class AppViewProgram
    {
        static async Task Main(string[] args)
        {
            using var relationships = new BlueskyRelationships();
            using var apis = new BlueskyEnrichedApis(relationships);
            Console.CancelKeyPress += (s, e) =>
            {

                relationships.NotifyShutdownRequested();
                Environment.Exit(0);
            };

            using var indexer = new Indexer(apis);
            Console.Error.WriteLine("Indexing the firehose to " + relationships.BaseDirectory + "...");
            Console.Error.WriteLine("NOTE: If you want to use the Web UI, run AppViewLite.Web instead.");
            Console.Error.WriteLine("Press CTRL+C to stop indexing...");
            //indexer.RetrievePlcDirectoryLoopAsync().FireAndForget();
            await indexer.StartListeningToJetstreamFirehose();
        }

    }

}

