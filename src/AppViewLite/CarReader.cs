using CarpaNet;
using CarpaNet.Cbor;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;

// Adapted from https://github.com/drasticactions/CarpaNet/blob/404690e48ec308f5b443643975f9b429e4c23d97/src/CarpaNet/Repo/CarReader.cs
// to support reading from Length-unaware stream

namespace AppViewLite;

/// <summary>
/// Reads CAR (Content Addressable aRchive) v1 files.
/// CAR files are used to serialize ATProtocol repositories.
/// </summary>
public sealed class CarReader : IDisposable
{
    private readonly Stream _stream;
    private readonly bool _leaveOpen;
    private CarHeader? _header;

    /// <summary>
    /// Creates a new CarReader from a stream.
    /// </summary>
    /// <param name="stream">The stream containing CAR data.</param>
    /// <param name="leaveOpen">Whether to leave the stream open when disposed.</param>
    public CarReader(Stream stream, bool leaveOpen = false)
    {
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        _leaveOpen = leaveOpen;
    }

    /// <summary>
    /// Creates a new CarReader from a byte array.
    /// </summary>
    /// <param name="data">The CAR data.</param>
    public CarReader(byte[] data) : this(new MemoryStream(data), leaveOpen: false)
    {
    }

    /// <summary>
    /// Gets the CAR header.
    /// </summary>
    public CarHeader Header
    {
        get
        {
            if (_header == null)
            {
                _header = ReadHeader();
            }
            return _header;
        }
    }

    /// <summary>
    /// Reads all blocks from the CAR file.
    /// </summary>
    /// <returns>An enumerable of all blocks.</returns>
    public IEnumerable<CarBlock> ReadBlocks()
    {
        // Ensure header is read first
        _ = Header;

        while (true)
        {
            var block = ReadNextBlock();
            if (block == null)
                yield break;
            yield return block;
        }
    }

    /// <summary>
    /// Reads all blocks into a dictionary keyed by CID.
    /// </summary>
    /// <returns>A dictionary mapping CID string to block data.</returns>
    public Dictionary<string, byte[]> ReadAllBlocks()
    {
        var blocks = new Dictionary<string, byte[]>();
        foreach (var block in ReadBlocks())
        {
            // Use the CID string as key (may have duplicates, last wins)
            blocks[block.Cid.Value] = block.Data;
        }
        return blocks;
    }

    private CarHeader ReadHeader()
    {
        // Read header length (varint)
        var headerLength = ReadVarint();
        if (headerLength == 0)
            throw new InvalidDataException("Invalid CAR header length");

        // Read header CBOR data
        var headerData = new byte[headerLength];
        var bytesRead = _stream.Read(headerData, 0, (int)headerLength);
        if (bytesRead != (int)headerLength)
            throw new InvalidDataException("Unexpected end of stream reading CAR header");

        // Parse header as DAG-CBOR
        return CarHeader.FromCbor(headerData);
    }

    private CarBlock? ReadNextBlock()
    {
        // Read block length (varint)
        var blockLength = TryReadVarint() ?? 0;
        if (blockLength == 0)
            return null;

        // Read block data (CID + data)
        var blockData = new byte[blockLength];
        _stream.ReadExactly(blockData);

        // Parse CID from the beginning of the block
        var (cid, cidLength) = ParseCidFromBytes(blockData);

        // The rest is the actual data
        var data = new byte[(int)blockLength - cidLength];
        Array.Copy(blockData, cidLength, data, 0, data.Length);

        return new CarBlock(cid, data);
    }

    private (ATCid Cid, int Length) ParseCidFromBytes(byte[] data)
    {
        // CID format:
        // - CIDv1: version (1) + multicodec (varint) + multihash (type + length + hash)
        // - CIDv0: just multihash (starts with 0x12 0x20 for sha256)

        if (data.Length < 2)
            throw new InvalidDataException("Block too small to contain CID");

        var offset = 0;

        // Check for CIDv0 (starts with multihash directly: 0x12 = sha256, 0x20 = 32 bytes)
        if (data[0] == 0x12 && data[1] == 0x20)
        {
            // CIDv0: multihash only (34 bytes for sha256)
            var cidLength = 2 + 32; // type + length + hash
            if (data.Length < cidLength)
                throw new InvalidDataException("Invalid CIDv0 length");

            var cidBytes = new byte[cidLength];
            Array.Copy(data, cidBytes, cidLength);
            var cid = ATCid.FromBytes(cidBytes);
            return (cid, cidLength);
        }

        // CIDv1: version + multicodec + multihash
        var version = ReadVarintFromSpan(data, ref offset);
        if (version != 1)
            throw new InvalidDataException($"Unsupported CID version: {version}");

        var multicodec = ReadVarintFromSpan(data, ref offset);

        // Read multihash
        var multihashType = ReadVarintFromSpan(data, ref offset);
        var multihashLength = ReadVarintFromSpan(data, ref offset);

        var totalCidLength = offset + (int)multihashLength;
        if (data.Length < totalCidLength)
            throw new InvalidDataException("Invalid CID multihash length");

        var cidBytesV1 = new byte[totalCidLength];
        Array.Copy(data, cidBytesV1, totalCidLength);
        var cidV1 = ATCid.FromBytes(cidBytesV1);
        return (cidV1, totalCidLength);
    }

    private ulong ReadVarint()
    {
        ulong value = 0;
        int shift = 0;

        while (true)
        {
            var b = _stream.ReadByte();
            if (b < 0)
                throw new InvalidDataException("Unexpected end of stream reading varint");

            value |= (ulong)(b & 0x7F) << shift;

            if ((b & 0x80) == 0)
                return value;

            shift += 7;
            if (shift > 63)
                throw new InvalidDataException("Varint overflow");
        }
    }

    private ulong? TryReadVarint()
    {
        ulong value = 0;
        int shift = 0;

        while (true)
        {
            var b = _stream.ReadByte();
            if (b < 0)
            {
                if (shift == 0) return null;
                throw new InvalidDataException("Unexpected end of stream reading varint");
            }

            value |= (ulong)(b & 0x7F) << shift;

            if ((b & 0x80) == 0)
                return value;

            shift += 7;
            if (shift > 63)
                throw new InvalidDataException("Varint overflow");
        }
    }

    private static ulong ReadVarintFromSpan(byte[] data, ref int offset)
    {
        ulong value = 0;
        int shift = 0;

        while (offset < data.Length)
        {
            var b = data[offset++];
            value |= (ulong)(b & 0x7F) << shift;

            if ((b & 0x80) == 0)
                return value;

            shift += 7;
            if (shift > 63)
                throw new InvalidDataException("Varint overflow");
        }

        throw new InvalidDataException("Unexpected end of data reading varint");
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (!_leaveOpen)
        {
            _stream.Dispose();
        }
    }
}

/// <summary>
/// Represents the header of a CAR file.
/// </summary>
public sealed class CarHeader
{
    /// <summary>
    /// The CAR format version (should be 1 for CARv1).
    /// </summary>
    public int Version { get; }

    /// <summary>
    /// The root CIDs. The first root is typically the commit CID.
    /// </summary>
    public IReadOnlyList<ATCid> Roots { get; }

    /// <summary>
    /// Creates a new CarHeader.
    /// </summary>
    public CarHeader(int version, IReadOnlyList<ATCid> roots)
    {
        Version = version;
        Roots = roots;
    }

    /// <summary>
    /// Parses a CarHeader from CBOR data.
    /// </summary>
    internal static CarHeader FromCbor(byte[] data)
    {
        var reader = new DagCborReader(data);

        var count = reader.ReadStartMap();
        int version = 1;
        var roots = new List<ATCid>();

        var remaining = count ?? int.MaxValue;
        while (remaining > 0 && reader.PeekState() != System.Formats.Cbor.CborReaderState.EndMap)
        {
            var key = reader.ReadTextString();

            switch (key)
            {
                case "version":
                    version = reader.ReadInt32();
                    break;

                case "roots":
                    var rootCount = reader.ReadStartArray();
                    var rootRemaining = rootCount ?? int.MaxValue;
                    while (rootRemaining > 0 && reader.PeekState() != System.Formats.Cbor.CborReaderState.EndArray)
                    {
                        var cid = reader.ReadCidLink();
                        roots.Add(cid);
                        rootRemaining--;
                    }
                    reader.ReadEndArray();
                    break;

                default:
                    reader.SkipValue();
                    break;
            }

            remaining--;
        }

        reader.ReadEndMap();

        if (version != 1)
            throw new InvalidDataException($"Unsupported CAR version: {version}");

        return new CarHeader(version, roots);
    }
}

/// <summary>
/// Represents a single block in a CAR file.
/// </summary>
public sealed class CarBlock
{
    /// <summary>
    /// The CID (content identifier) of this block.
    /// </summary>
    public ATCid Cid { get; }

    /// <summary>
    /// The raw data of this block (typically DAG-CBOR encoded).
    /// </summary>
    public byte[] Data { get; }

    /// <summary>
    /// Creates a new CarBlock.
    /// </summary>
    public CarBlock(ATCid cid, byte[] data)
    {
        Cid = cid;
        Data = data;
    }
}

