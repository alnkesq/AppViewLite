using FishyFlip.Lexicon;
using FishyFlip.Tools;
using Ipfs;
using PeterO.Cbor;
using AppViewLite.Numerics;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using DuckDbSharp.Types;
using System.Threading;
using FishyFlip.Models;

namespace AppViewLite
{
    public class CarImporter : IDisposable
    {
        private readonly string Did;
        private readonly Dictionary<DuckDbUuid, RecordLocation> recordsByCid = new();
        private readonly List<(string Collection, string RKey, DuckDbUuid CidHash)> pathToCid = new();
        private readonly string logPrefix;

        private FileStream? spilledRecords;
        private readonly string diskSpillDirectory;

        private record struct RecordLocation(ATObject? InMemory, long SpilledStart, int SpilledLength);

        public CarImporter(string did, DateTime probableDateOfEarliestRecord, string diskSpillDirectory)
        {
            if (!did.StartsWith("did:", StringComparison.Ordinal)) AssertionLiteException.Throw("Bad DID passed to CarImporter: " + did);
            Did = did;
            logPrefix = "ImportCar: " + did + ": ";
            ProbableDateOfEarliestRecord = probableDateOfEarliestRecord;
            this.diskSpillDirectory = diskSpillDirectory;
        }

        public void Log(string v)
        {
            LoggableBase.LogInfo(logPrefix + v);
        }

        public Tid LargestSeenRev;

        public DateTime ProbableDateOfEarliestRecord;
        public DateTime? LargestSeenRecordDate;
        public DateTime? SmallestSeenRecordDate;
        public int TotalRecords => pathToCid.Count;

        public double EstimatedRetrievalProgress
        {
            get
            {
                var largestSeenRecordDate = (LargestSeenRecordDate ?? DateTime.UtcNow);
                var smallestSeenRecordDate = SmallestSeenRecordDate ?? largestSeenRecordDate;

                var lifespan = largestSeenRecordDate - ProbableDateOfEarliestRecord;

                var positionSinceProbableCreation = smallestSeenRecordDate - ProbableDateOfEarliestRecord;
                var ratioSinceProbableCreation = (double)positionSinceProbableCreation.Ticks / lifespan.Ticks;
                return Math.Clamp(1 - ratioSinceProbableCreation, 0.01, 0.99);
            }
        }


        private readonly static int CarSpillToDiskBytes = AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_CAR_SPILL_TO_DISK_BYTES) ?? (64 * 1024 * 1024);

        private long currentImportDecodedBytes;
        internal static long GlobalDecodedBytes;

        private HashSet<string> internedCollectionNames = new();
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

            Interlocked.Add(ref currentImportDecodedBytes, p.Bytes.Length);
            Interlocked.Add(ref GlobalDecodedBytes, p.Bytes.Length);

            if (blockObj["$type"] is not null)
            {
                if (GlobalDecodedBytes > CarSpillToDiskBytes)
                {
                    if (spilledRecords == null)
                    {
                        Directory.CreateDirectory(diskSpillDirectory);
                        spilledRecords = new FileStream(Path.Combine(diskSpillDirectory, Guid.NewGuid() + ".tmp"), FileMode.Create, FileAccess.ReadWrite, FileShare.Delete, 4096, FileOptions.DeleteOnClose);
                    }
                    recordsByCid[CidToUuid(p.Cid)] = new RecordLocation(null, spilledRecords.Position, p.Bytes.Length);
                    spilledRecords.Write(p.Bytes);
                }
                else
                {
                    //Console.Error.WriteLine("Record: " + blockObj["$type"] + " cid: " + p.Cid);
                    try
                    {
                        recordsByCid[CidToUuid(p.Cid)] = new RecordLocation(blockObj.ToATObject(), default, default);

                    }
                    catch (Exception ex)
                    {
                        Log(blockObj["$type"] + " " + ex.Message);
                        return;
                    }
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
                    if (prev > buf.Length) AssertionLiteException.Throw("CAR: p is beyond previous buffer");
                    buf.Length = prev;
                    buf.Append(compressedPath);
                    var path = buf.ToString();
                    var val = ee["v"].GetByteString();
                    if (val[0] != 0) throw AssertionLiteException.Throw("CAR: v is not zero");
                    var valCid = ATCid.Read(val.AsSpan(1).ToArray());
                    var bak = valCid.ToBytes();
                    var slash = path.IndexOf('/');
                    var collection = path.Substring(0, slash);
                    var rkey = path.Substring(slash + 1);

                    //Console.Error.WriteLine((Tid.TryParse(rkey, out var t) ? t.Date : default) + "Path:   " + collection + " cid: " + valCid);

                    if (Tid.TryParse(rkey, out var tid) && collection is
                        FishyFlip.Lexicon.App.Bsky.Graph.Follow.RecordType or
                        FishyFlip.Lexicon.App.Bsky.Graph.Block.RecordType or
                        FishyFlip.Lexicon.App.Bsky.Feed.Like.RecordType or
                        FishyFlip.Lexicon.App.Bsky.Feed.Repost.RecordType or
                        FishyFlip.Lexicon.App.Bsky.Feed.Post.RecordType)
                    {
                        var date = tid.Date;
                        //if (SmallestSeenRecordDate == null ||date < SmallestSeenRecordDate)
                        //{

                        SmallestSeenRecordDate = date; // mostly descending order

                        //}
                        if (LargestSeenRecordDate == null || date > LargestSeenRecordDate)
                        {
                            LargestSeenRecordDate = date;
                        }
                        //if (date < ProbableDateOfEarliestRecord)
                        //{
                        //    ProbableDateOfEarliestRecord = date;
                        //}


                    }
                    if (internedCollectionNames.TryGetValue(collection, out var interned))
                        collection = interned;
                    else
                        internedCollectionNames.Add(collection);
                    pathToCid.Add((collection, rkey, CidToUuid(valCid)));
                }

            }
        }

        public void LogStats()
        {
            Log("Read " + pathToCid.Count + " CIDs, " + recordsByCid.Count + " records. Populating...");
        }

        public IEnumerable<(string Did, string Path, ATObject Record)> EnumerateRecords()
        {
            foreach (var item in pathToCid.DistinctBy(x => (x.CidHash, x.RKey, x.Collection)).OrderBy(x => x.RKey))
            {
                if (!recordsByCid.TryGetValue(item.CidHash, out var recordLocation)) continue;

                var record = TryReadRecord(recordLocation);
                if (record != null)
                    yield return (Did, item.Collection + "/" + item.RKey, record);
            };
        }

        private ATObject? TryReadRecord(RecordLocation recordLocation)
        {
            if (recordLocation.InMemory != null) return recordLocation.InMemory;
            var bytes = new byte[recordLocation.SpilledLength];
            spilledRecords!.Seek(recordLocation.SpilledStart, SeekOrigin.Begin);
            spilledRecords.ReadExactly(bytes);
            var cbor = CBORObject.DecodeFromBytes(bytes); // This succeeded during OnCarDecoded
            try
            {
                return cbor.ToATObject();
            }
            catch (Exception ex)
            {
                Log(cbor["$type"] + " " + ex.Message);
                return null;
            }
        }

        private static DuckDbUuid CidToUuid(ATCid cid) => StringUtils.HashToUuid(cid.ToBytes());

        public void Dispose()
        {
            spilledRecords?.Dispose();
            recordsByCid.Clear();
            pathToCid.Clear();

            while (true)
            {
                var decoded = Interlocked.Read(ref currentImportDecodedBytes);
                if (decoded == 0) break;
                if (Interlocked.CompareExchange(ref currentImportDecodedBytes, 0, decoded) == decoded)
                {
                    Interlocked.Add(ref GlobalDecodedBytes, -decoded);
                    break;
                }
            }
        }
    }
    public record struct CarImportProgress(long DownloadedBytes, long EstimatedTotalBytes, long InsertedRecords, long TotalRecords, Tid LastRecordRkey);
}

