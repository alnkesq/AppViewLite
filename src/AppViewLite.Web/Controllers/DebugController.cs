using AppViewLite.Models;
using Microsoft.AspNetCore.Mvc;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;

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
            ctx.EnsureAdministrator();

            return apis.WithRelationshipsLockForDids(dids, (plcs, rels) => dids.Select((x, i) => new { did = x, plcId = plcs[i].PlcValue }).ToArray(), ctx);
        }
        [HttpGet("dids-to-plc-ids")]
        public object DidsToPlcIds(string dids)
        {
            ctx.EnsureAdministrator();

            return DidsToPlcIds(dids.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries));
        }

        [HttpPost("plc-ids-to-dids")]
        public object PlcIdsToDids([FromBody] long[] ids)
        {
            ctx.EnsureAdministrator();

            return apis.WithRelationshipsLock(rels => ids.Select(x =>
            {
                var did = rels.TryGetDid(new Plc(checked((int)x)));
                return new { did = did, plcId = x };
            }).ToArray(), ctx);
        }
        [HttpGet("plc-ids-to-dids")]
        public object PlcIdsToDids(string ids)
        {
            ctx.EnsureAdministrator();

            return PlcIdsToDids(ids.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries).Select(long.Parse).ToArray());
        }

        [HttpPost("global-flush")]
        public void GlobalFlush()
        {
            ctx.EnsureAdministrator();

            apis.WithRelationshipsWriteLock(rels => rels.GlobalFlush(), ctx);
        }



        [HttpGet("table/{table}")]
        public object? Lookup(string table, string? key, int reverse = 0, string? start = null, int limit = 1000, string? proto = null)
        {
            ctx.EnsureAdministrator();

            var combinedTable = apis.DangerousUnlockedRelationships.AllMultidictionaries.First(x => Path.GetFileName(x.DirectoryPath) == table);
            var genericArgs = combinedTable.GetType().GetGenericArguments();
            var keyType = genericArgs[0];
            var valueType = genericArgs[1];

            var protoType = proto != null ? typeof(Plc).Assembly.GetType(proto, throwOnError: true) : null;
            if (key != null)
            {
                return apis.WithRelationshipsLock<object?>(rels =>
                {
                    object keyParsed = StringUtils.ParseRecord(keyType, key);
                    object? startParsed = start != null ? StringUtils.ParseRecord(valueType, start) : null;
                    if (start == null && combinedTable.Behavior == AppViewLite.Storage.PersistentDictionaryBehavior.PreserveOrder)
                    {
                        var bytes = combinedTable.GetValuesPreserveOrderUntyped(keyParsed);
                        if (protoType != null)
                        {
                            var obj = ProtoBuf.Serializer.Deserialize(protoType, new MemoryStream((byte[])bytes));

                            var settings = new JsonSerializerOptions { Converters = { (JsonConverter)Activator.CreateInstance(typeof(PrivateFieldSerializer<>).MakeGenericType(protoType))! } };
                            var j = System.Text.Json.JsonSerializer.Serialize(obj, settings);
                            var doc = JsonDocument.Parse(j);
                            JsonElement root = doc.RootElement;
                            return root;
                        }
                        return bytes;
                    }

                    if (proto != null) throw new ArgumentException("proto is only supported with PreserveOrder tables.");

                    var results = reverse != 0 ? combinedTable.GetValuesSortedDescendingUntyped(keyParsed, startParsed) : combinedTable.GetValuesSortedUntyped(keyParsed, startParsed);
                    return results.Cast<object>().Take(limit).ToArray();
                }, ctx);
            }
            else
            {
                if (proto != null)
                    throw new ArgumentException("Proto deserialization requires a key lookup.");

                return apis.WithRelationshipsLock(rels =>
                {
                    object? startParsed = start != null ? StringUtils.ParseRecord(keyType, start) : null;

                    var results = combinedTable.EnumerateSortedDescendingUntyped(startParsed);
                    return results.Cast<object>().Take(limit).ToArray();
                }, ctx);
            }
        }

        internal class PrivateFieldSerializer<T> : JsonConverter<T>
        {
            public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                throw new NotSupportedException("PrivateFieldSerializer does not support reading.");
            }

            public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
            {
                writer.WriteStartObject();
                Type type = typeof(T);

                foreach (FieldInfo field in type.GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public))
                {
                    var fieldValue = field.GetValue(value);
                    writer.WritePropertyName(field.Name);
                    JsonSerializer.Serialize(writer, fieldValue, field.FieldType, options);
                }

                writer.WriteEndObject();
            }
        }
    }
}

