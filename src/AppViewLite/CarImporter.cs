using FishyFlip.Lexicon;
using FishyFlip.Lexicon.App.Bsky.Actor;
using FishyFlip.Tools;
using Ipfs;
using PeterO.Cbor;
using AppViewLite.Numerics;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    public class CarImporter
    {
        private readonly string Did;
        private readonly Dictionary<Cid, ATObject> recordsByCid;
        private readonly List<(string Collection, string RKey, Cid Cid)> pathToCid;
        private readonly string logPrefix;

        public CarImporter(string did)
        {
            if (!did.StartsWith("did:", StringComparison.Ordinal)) throw new ArgumentException();
            Did = did;
            recordsByCid = new Dictionary<Cid, ATObject>();
            pathToCid = new List<(string Collection, string RKey, Cid Cid)>();
            logPrefix = "ImportCar: " + did + ": ";
        }

        public void Log(string v)
        {
            LoggableBase.LogInfo(logPrefix + v);
        }

        public Tid LargestSeenRev;

        public void OnCarDecoded(CarProgressStatusEvent p)
        {


            using var blockStream = new MemoryStream(p.Bytes.ToArray());
            CBORObject blockObj;
            try
            {
                blockObj = CBORObject.Read(blockStream);
            }
            catch (Exception ex)
            {
                Log("Unable to deserialize CBOR: " + ex.Message);
                return;
            }
            if (blockObj["$type"] is not null)
            {
                try
                {
                    recordsByCid[p.Cid] = blockObj.ToATObject();

                }
                catch (Exception ex)
                {
                    Log(blockObj["$type"] + " " + ex.Message);
                    return;
                }
            }
            else if (blockObj["sig"] is not null)
            {
                var rev = blockObj["rev"].AsString();
                var parsedRev = Tid.Parse(rev);
                if (parsedRev.TidValue > LargestSeenRev.TidValue)
                {
                    LargestSeenRev = parsedRev;
                }
            }
            else
            {
                var buf = new StringBuilder();
                var e = blockObj["e"];
                for (int i = 0; i < e.Count; i++)
                {
                    var ee = e[i];
                    var compressedPath = Encoding.UTF8.GetString(ee["k"].GetByteString());
                    var prev = ee["p"].AsInt32();
                    if (prev > buf.Length) throw new Exception();
                    buf.Length = prev;
                    buf.Append(compressedPath);
                    var path = buf.ToString();
                    var val = ee["v"].GetByteString();
                    if (val[0] != 0) throw new Exception();
                    var valCid = Cid.Read(val.AsSpan(1).ToArray());
                    var bak = valCid.ToArray();
                    var slash = path.IndexOf('/');
                    var collection = path.Substring(0, slash);
                    var rkey = path.Substring(slash + 1);
                    pathToCid.Add((collection, rkey, valCid));
                }

            }
        }

        public void LogStats()
        {
            Log("Read " + pathToCid.Count + " CIDs, " + recordsByCid.Count + " records. Populating...");
        }

        public IEnumerable<(string Did, string Path, ATObject Record)> EnumerateRecords()
        {
            foreach (var item in pathToCid.DistinctBy(x => (x.Cid, x.RKey, x.Collection)).OrderBy(x => x.RKey))
            {
                if (!recordsByCid.TryGetValue(item.Cid, out var record)) continue;

                yield return (Did, item.Collection + "/" + item.RKey, record);
            };
        }

    }
    public record struct CarImportProgress(long DownloadedBytes, long RecordCount, Tid LastSeenRev);
}

