using AppViewLite.Models;
using AppViewLite.Storage;
using DuckDbSharp.Types;
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
            LoggableBase.Initialize();
            using var relationships = new BlueskyRelationships();
            using var apis = new BlueskyEnrichedApis(relationships);
            Console.CancelKeyPress += (s, e) =>
            {

                relationships.NotifyShutdownRequested();
                Environment.Exit(0);
            };

            using var indexer = new Indexer(apis);
            LoggableBase.Log("Indexing the firehose to " + relationships.BaseDirectory + "...");
            LoggableBase.Log("NOTE: If you want to use the Web UI, run AppViewLite.Web instead.");
            LoggableBase.Log("Press CTRL+C to stop indexing...");
            //indexer.RetrievePlcDirectoryLoopAsync().FireAndForget();
            indexer.VerifyValidForCurrentRelay = x => { };
            await indexer.StartListeningToAtProtoFirehoseRepos();
        }

    }

}

