using AppViewLite.Numerics;
using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    [ProtoContract]
    public class RepositoryImportEntry
    {
        [ProtoMember(1)] public RepositoryImportKind Kind;
        [ProtoMember(2)] public long DurationMillis;
        [ProtoMember(3)] public long LastRevOrTid;
        [ProtoMember(4)] public string? Error;
        [ProtoMember(5)] public long StartRevOrTid;
        [ProtoMember(6)] public long DownloadedBytes;
        [ProtoMember(7)] public long InsertedRecordCount;

        public DateTime StartDate;
        public Plc Plc;
        public bool StillRunning;
        public long EstimatedTotalBytes;
        public long TotalRecords;

        public string DisplayText
        {
            get
            {
                var suffix = " · " + (StartRevOrTid != default ? "incremental since " + StringUtils.ToHumanDate(new Tid(StartRevOrTid).Date, true) : "full retrieval");
                if (Error != null)
                {
                    return "Error: " + Error + suffix;
                }
                else if (StillRunning)
                {
                    if (InsertedRecordCount != 0)
                    {
                        if (TotalRecords != 0)
                        {
                            return $"Downloaded: 100% ({StringUtils.ToHumanBytes(EstimatedTotalBytes)}), Inserting: {StringUtils.FormatPercent(InsertedRecordCount, TotalRecords)} ({InsertedRecordCount} records){suffix}";
                        }
                        else
                        {

                            return $"Inserting: {InsertedRecordCount} records{suffix}";
                        }
                    }
                    else
                    {
                        if (EstimatedTotalBytes == 0)
                        {
                            return $"Downloading: {StringUtils.FormatPercent(DownloadedBytes, EstimatedTotalBytes)} of (unknown){suffix}";
                        }
                        else
                        {
                            return $"Downloading: {StringUtils.FormatPercent(DownloadedBytes, EstimatedTotalBytes)} of ~{StringUtils.ToHumanBytes(EstimatedTotalBytes)}{suffix}";
                        }
                    }

                }
                else
                {
                    return $"Completed in {(DurationMillis / 1000).ToString("0.0")} seconds ({InsertedRecordCount} records{(DownloadedBytes != 0 ? ", " + StringUtils.ToHumanBytes(DownloadedBytes) : null)}){suffix}";
                }

            }
        }

    }


    public enum RepositoryImportKind
    { 
        None,
        CAR,
        Posts,
        Likes,
        Follows,
        Reposts,
        Blocks,
        ListMetadata,
        ListEntries,
        BlocklistSubscriptions,
        FeedGenerators,
        Threadgates,
        Postgates,
    }

}

