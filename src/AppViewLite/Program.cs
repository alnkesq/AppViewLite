using System;
using System.Threading.Tasks;

namespace AppViewLite
{


    public class AppViewProgram
    {
        static async Task Main(string[] args)
        {
            AppViewLiteConfiguration.ReadEnvAndArgs(args);
            LoggableBase.Initialize();
            using var relationships = new BlueskyRelationships();
            BlueskyRelationships.CreateTimeSeries();
            using var primarySecondaryPair = new PrimarySecondaryPair(relationships);
            var apis = new BlueskyEnrichedApis(primarySecondaryPair);
            Console.CancelKeyPress += (s, e) =>
            {

                relationships.NotifyShutdownRequested();
                Environment.Exit(0);
            };

            var indexer = new Indexer(apis);
            LoggableBase.Log("Indexing the firehose to " + relationships.BaseDirectory + "...");
            LoggableBase.Log("NOTE: If you want to use the Web UI, run AppViewLite.Web instead.");
            LoggableBase.Log("Press CTRL+C to stop indexing...");
            //indexer.RetrievePlcDirectoryLoopAsync().FireAndForget();
            indexer.VerifyValidForCurrentRelay = x => { };
            await indexer.StartListeningToAtProtoFirehoseRepos();
        }

    }

}

