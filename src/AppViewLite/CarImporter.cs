using AppBsky.Feed;
using AppViewLite.Models;
using AppViewLite.Numerics;
using CarpaNet;
using CarpaNet.Cbor;
using CarpaNet.Repo;
using DuckDbSharp.Types;
using AppViewLite.Storage;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;

namespace AppViewLite
{
    public sealed class CarImporter : IDisposable
    {
        private readonly string Did;
        private readonly Dictionary<DuckDbUuid, RecordLocation> recordsByCid = [];
        private readonly List<(string Collection, string RKey, DuckDbUuid CidHash)> pathToCid = [];
        private readonly string logPrefix;

        private FileStream? spilledRecords;
        private readonly string diskSpillDirectory;

        private record struct RecordLocation(object? InMemory, long SpilledStart, int SpilledLength);

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

        private readonly HashSet<string> internedCollectionNames = [];



        public void OnCarDecoded(ATCid cid, byte[] bytes)
        {

            object? asRecord = null;
            MstNode? asNode = null;
            RepoCommit? asCommit = null;

            string? recordType = null;
            var isRecord = false;
            var isCommit = false;
            var isNode = false;

            try
            {
                var cborReader = new DagCborReader(bytes);

                cborReader.ReadStartMap();
                while (true)
                {

                    var p = cborReader.PeekState();
                    if (p == System.Formats.Cbor.CborReaderState.TextString)
                    {
                        var propname = cborReader.ReadTextString();
                        if (propname == "$type") { isRecord = true; recordType = cborReader.ReadTextString(); continue; }
                        else if (propname == "sig") isCommit = true;
                        else if (propname == "e") isNode = true;

                        cborReader.SkipValue();
                    }
                    else if (p == System.Formats.Cbor.CborReaderState.EndMap) break;
                    else throw new Exception("Unexpected CBOR key type: " + p);
                }

                if (isRecord) asRecord = Indexer.TryDeserializeCbor(bytes, recordType!);
                else if (isCommit) asCommit = RepoCommit.FromCbor(bytes);
                else if (isNode) asNode = MstNode.FromCbor(bytes);
                else throw new UnexpectedFirehoseDataException("Unknown CBOR entry type in CAR.");
            }
            catch (Exception ex)
            {
                Log("Unable to deserialize CBOR: " + ex.Message);
                return;

            }
            
            try
            {
                var cborReader = new DagCborReader(bytes);
                var propCount = cborReader.ReadStartMap();


                //blockObj = Indexer.TryDeserializeCbor(bytes);
            }
            catch (Exception ex)
            {
                Log("Unable to deserialize CBOR: " + ex.Message);
                return;
            }

            Interlocked.Add(ref currentImportDecodedBytes, bytes.Length);
            Interlocked.Add(ref GlobalDecodedBytes, bytes.Length);

            if (isRecord)
            {
                if (GlobalDecodedBytes > CarSpillToDiskBytes)
                {
                    if (spilledRecords == null)
                    {
                        Directory.CreateDirectory(diskSpillDirectory);
                        spilledRecords = new FileStream(Path.Combine(diskSpillDirectory, Guid.NewGuid() + ".tmp"), FileMode.Create, FileAccess.ReadWrite, FileShare.Delete, 4096, FileOptions.DeleteOnClose);
                    }
                    recordsByCid[CidToUuid(cid)] = new RecordLocation(null, spilledRecords.Position, bytes.Length);
                    spilledRecords.Write(bytes);
                }
                else
                {
                    //Console.Error.WriteLine("Record: " + blockObj["$type"] + " cid: " + p.Cid);
                    try
                    {
                        recordsByCid[CidToUuid(cid)] = new RecordLocation(asRecord ?? bytes, default, default);

                    }
                    catch (Exception ex)
                    {
                        Log(recordType + " " + ex.Message);
                        return;
                    }
                }
            }
            else if (asCommit != null)
            {
                var rev = asCommit.Rev;
                var parsedRev = Tid.Parse(rev);
                if (parsedRev.TidValue > LargestSeenRev.TidValue)
                {
                    LargestSeenRev = parsedRev;
                }
            }
            else if (asNode != null)
            {
                foreach (var ee in asNode.Entries)
                {

                    var valCid = ee.Value;
                    var path = CollectionAndRKey.ParseUnprefixed(ee.Key);

                    if (Tid.TryParse(path.RKey, out var tid) && path.Collection is
                        AppBsky.Graph.Follow.RecordType or
                        AppBsky.Graph.Block.RecordType or
                        AppBsky.Feed.Like.RecordType or
                        AppBsky.Feed.Repost.RecordType or
                        AppBsky.Feed.Post.RecordType)
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
                    if (internedCollectionNames.TryGetValue(path.Collection, out var interned))
                        path.Collection = interned;
                    else
                        internedCollectionNames.Add(path.Collection);
                    pathToCid.Add((path.Collection, path.RKey, CidToUuid(valCid)));
                }

            }

        }

        public void LogStats()
        {
            Log("Read " + pathToCid.Count + " CIDs, " + recordsByCid.Count + " records. Populating...");
        }

        public IEnumerable<(string Did, string Collection, string RKey, object Record)> EnumerateRecords()
        {
            foreach (var item in pathToCid.DistinctBy(x => (x.CidHash, x.RKey, x.Collection)).OrderBy(x => x.RKey))
            {
                if (!recordsByCid.TryGetValue(item.CidHash, out var recordLocation)) continue;

                var record = TryReadRecord(recordLocation, item.Collection);
                if (record != null)
                    yield return (Did, item.Collection, item.RKey, record);
            }
        }

        private object? TryReadRecord(RecordLocation recordLocation, string collection)
        {
            if (recordLocation.InMemory != null) return recordLocation.InMemory;
            var bytes = new byte[recordLocation.SpilledLength];
            spilledRecords!.Seek(recordLocation.SpilledStart, SeekOrigin.Begin);
            spilledRecords.ReadExactly(bytes);
            
            try
            {
                return Indexer.TryDeserializeCbor(bytes, collection);
            }
            catch (Exception ex)
            {
                Log(collection + " " + ex.Message);
                return null;
            }
        }


        private static DuckDbUuid CidToUuid(ATCid cid) => StringUtils.HashToUuid(cid.ToBytesV2());

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

