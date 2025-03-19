using AppViewLite.Models;
using Google.Protobuf.WellKnownTypes;
using Microsoft.AspNetCore.Mvc;

namespace AppViewLite.Web.Controllers
{
    [Route("/api/debug")]
    [ApiController]
    public class DebugController
    {
        private readonly RequestContext ctx;
        private readonly BlueskyEnrichedApis apis;

        public DebugController(RequestContext requestContext, BlueskyEnrichedApis apis)
        {
            this.ctx = requestContext;
            this.apis = apis;
        }

        [HttpPost("dids-to-plc-ids")]
        public object DidsToPlcIds([FromBody] string[] dids)
        {
            return apis.WithRelationshipsLockForDids(dids, (plcs, rels) => dids.Select((x, i) => new { did = x, plcId = plcs[i].PlcValue }).ToArray(), ctx);
        }
        [HttpGet("dids-to-plc-ids")]
        public object DidsToPlcIds(string dids)
        {
            return DidsToPlcIds(dids.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries));
        }

        [HttpPost("plc-ids-to-dids")]
        public object PlcIdsToDids([FromBody] long[] ids)
        {
            return apis.WithRelationshipsLock(rels => ids.Select(x =>
            {
                var did = rels.TryGetDid(new Plc(checked((int)x)));
                return new { did = did, plcId = x };
            }).ToArray(), ctx);
        }
        [HttpGet("plc-ids-to-dids")]
        public object PlcIdsToDids(string ids)
        {
            return PlcIdsToDids(ids.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries).Select(long.Parse).ToArray());
        }

        [HttpPost("global-flush")]
        public void GlobalFlush()
        {
            apis.WithRelationshipsWriteLock(rels => rels.GlobalFlush(), ctx);
        }

        [HttpGet("table/{table}")]
        public object Lookup(string table, string key, bool reverse = false, string? start = null)
        {
            var combinedTable = apis.DangerousUnlockedRelationships.AllMultidictionaries.First(x => Path.GetFileName(x.DirectoryPath) == table);
            var genericArgs = combinedTable.GetType().GetGenericArguments();
            var keyType = genericArgs[0];
            var valueType = genericArgs[1];

            return apis.WithRelationshipsLock(rels =>
            {
                object keyParsed = StringUtils.ParseRecord(keyType, key);
                object? startParsed = start != null ? StringUtils.ParseRecord(valueType, start) : null;
                var results = reverse ? combinedTable.GetValuesSortedDescendingUntyped(keyParsed, startParsed) : combinedTable.GetValuesSortedUntyped(keyParsed, startParsed);
                return results.Cast<object>().Take(1000).ToArray();
            }, ctx);
        }
    }
}

