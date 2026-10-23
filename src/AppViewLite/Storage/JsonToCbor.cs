using System;
using System.Formats.Cbor;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace AppViewLite.Storage;

public static class JsonCborUtils
{
    public static byte[] JsonToCbor(string json)
    {
        return JsonElement.Parse(json).JsonToCbor();
    }
    public static byte[] JsonToCbor(this JsonElement json)
    {
        var writer = new CborWriter();
        JsonToCbor(writer, json);
        return writer.Encode();
    }

    public static void JsonToCbor(CborWriter writer, JsonElement element)
    {


        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                
                writer.WriteStartMap(element.GetPropertyCount());
                foreach (var property in element.EnumerateObject())
                {
                    writer.WriteTextString(property.Name);
                    JsonToCbor(writer, property.Value);
                }
                writer.WriteEndMap();
                break;

            case JsonValueKind.Array:
                writer.WriteStartArray(element.GetArrayLength());
                foreach (var item in element.EnumerateArray())
                    JsonToCbor(writer, item);
                writer.WriteEndArray();
                break;

            case JsonValueKind.String:
                writer.WriteTextString(element.GetString()!);
                break;

            case JsonValueKind.Number:
                if (element.TryGetInt64(out long l))
                    writer.WriteInt64(l);
                else if (element.TryGetUInt64(out ulong ul))
                    writer.WriteUInt64(ul);
                else
                    writer.WriteDouble(element.GetDouble());
                break;

            case JsonValueKind.True:
                writer.WriteBoolean(true);
                break;

            case JsonValueKind.False:
                writer.WriteBoolean(false);
                break;

            case JsonValueKind.Null:
                writer.WriteNull();
                break;

            default:
                throw new JsonException($"Unsupported JSON value: {element.ValueKind}");
        }
    }


    public static string CborToJson(ReadOnlyMemory<byte> cbor)
    {
        var reader = new CborReader(cbor);
        using var stream = new MemoryStream();

        using (var writer = new Utf8JsonWriter(stream))
        {
            CborToJson(reader, writer);
        }

        return Encoding.UTF8.GetString(stream.ToArray());
    }

    static void CborToJson(CborReader reader, Utf8JsonWriter writer)
    {
        switch (reader.PeekState())
        {
            case CborReaderState.StartMap:
                writer.WriteStartObject();
                reader.ReadStartMap();
                while (reader.PeekState() != CborReaderState.EndMap)
                {
                    writer.WritePropertyName(reader.ReadTextString());
                    CborToJson(reader, writer);
                }
                reader.ReadEndMap();
                writer.WriteEndObject();
                break;

            case CborReaderState.StartArray:
                writer.WriteStartArray();
                reader.ReadStartArray();
                while (reader.PeekState() != CborReaderState.EndArray)
                    CborToJson(reader, writer);
                reader.ReadEndArray();
                writer.WriteEndArray();
                break;

            case CborReaderState.TextString:
                writer.WriteStringValue(reader.ReadTextString());
                break;

            case CborReaderState.UnsignedInteger:
                writer.WriteNumberValue(reader.ReadUInt64());
                break;

            case CborReaderState.NegativeInteger:
                writer.WriteNumberValue(reader.ReadCborNegativeIntegerRepresentation());
                break;

            case CborReaderState.ByteString:
                writer.WriteBase64StringValue(reader.ReadByteString());
                break;

            case CborReaderState.HalfPrecisionFloat:
            case CborReaderState.SinglePrecisionFloat:
            case CborReaderState.DoublePrecisionFloat:
                writer.WriteNumberValue(reader.ReadDouble());
                break;

            case CborReaderState.Boolean:
                writer.WriteBooleanValue(reader.ReadBoolean());
                break;

            case CborReaderState.Null:
                reader.ReadNull();
                writer.WriteNullValue();
                break;

            case CborReaderState.Finished:
                throw new FormatException("Unexpected end of CBOR data.");

            default:
                throw new FormatException(
                    $"Unsupported CBOR type: {reader.PeekState()}");
        }
    }
}

