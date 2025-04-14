using System;
using System.Threading.Tasks;

namespace AppViewLite
{


    public class AppViewProgram
    {
        static async Task Main(string[] args)
        {
            var apis = AppViewLiteInit.Init(args);

            Console.CancelKeyPress += (s, e) =>
            {
                BlueskyEnrichedApis.Instance.NotifyShutdownRequested();
                Environment.Exit(0);
            };

            var indexer = new Indexer(apis);
            LoggableBase.Log("You're running the AppViewLite firehose listener WITHOUT the AppViewLite server.");
            LoggableBase.Log("If you want to use the Web UI, run AppViewLite.Web instead.");
            LoggableBase.Log("Press CTRL+C to stop indexing...");
            //indexer.RetrievePlcDirectoryLoopAsync().FireAndForget();
            indexer.VerifyValidForCurrentRelay = x => { };
            await indexer.StartListeningToAtProtoFirehoseRepos(retryPolicy: null);
        }

    }

}

