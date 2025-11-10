using System;
using System.Collections.Generic;
using System.IO.Hashing;
using System.Linq;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    public class ProbabilisticSet<T> where T : unmanaged
    {
        public readonly ulong[] Array;
        public Span<byte> ArrayAsBytes => MemoryMarshal.AsBytes<ulong>((Span<ulong>)Array);
        private readonly int _hashFunctions;
        private readonly int _bitsPerFunction;
        private readonly ulong _getFunctionBitsMask;
        private const int BITS_PER_WORD = 64;
        private const int BIT_SHIFT = 6;
        private const int BYTES_PER_WORD = 8;
        private const ulong MASK = ((ulong)BITS_PER_WORD - 1);
        public long SizeInBytes => (long)Array.Length * BYTES_PER_WORD;
        public int BitsPerFunction => _bitsPerFunction;
        public int HashFunctions => _hashFunctions;

        public HitMissCounter RuleOutCounter = new HitMissCounter();

        public ProbabilisticSet(ProbabilisticSetParameters parameters)
            : this(parameters.SizeInBytes, parameters.HashFunctions)
        { 
        
        }
        public ProbabilisticSet(long sizeInBytes, int hashFunctions)
            : this(new ulong[checked((int)(sizeInBytes / BYTES_PER_WORD))], hashFunctions)
        {
        }
        public ProbabilisticSet(ulong[] array, int hashFunctions)
        {
            if (!BitOperations.IsPow2(array.Length)) throw new ArgumentException();
            var arrayLengthInBits = (ulong)array.Length * BITS_PER_WORD;
            _bitsPerFunction = ProbabilisticSetParameters.GetBitsPerFunction(arrayLengthInBits);
            Array = array;
            _hashFunctions = hashFunctions;
            _getFunctionBitsMask = (1ul << _bitsPerFunction) - 1;
        }

        public bool PossiblyContains(T value)
        {
            var enumerator = EnumerateHashBits(GetHash(in value));
            for (int i = 0; i < _hashFunctions; i++)
            {
                ulong bitIndex = GetNextIndex(ref enumerator);

                ulong wordIndex = bitIndex >> BIT_SHIFT;
                ulong bitIndexWithinWord = bitIndex & MASK;
                ulong wordTest = 1ul << ((int)bitIndexWithinWord);

                var word = Array[wordIndex];
                if ((word & wordTest) == 0)
                {
                    RuleOutCounter.OnHit();
                    return false;
                }
            }
            RuleOutCounter.OnMiss();
            return true;
        }



        public void Add(in T value)
        {
            var enumerator = EnumerateHashBits(GetHash(in value));
            for (int i = 0; i < _hashFunctions; i++)
            {
                ulong bitIndex = GetNextIndex(ref enumerator);

                ulong wordIndex = bitIndex >> BIT_SHIFT;
                ulong bitIndexWithinWord = bitIndex & MASK;
                ulong wordOr = 1ul << ((int)bitIndexWithinWord);
                //Console.WriteLine($"{bitIndex} = {wordIndex * 64} + {bitIndexWithinWord}");
                Array[(int)wordIndex] |= wordOr;
            }
        }
        private ulong GetNextIndex(ref HashBitEnumerator enumerator)
        {
            if (enumerator.RemainingBits < _bitsPerFunction)
            {
                RefillHashEnumerator(ref enumerator);
            }


            var result = enumerator.NextBits & _getFunctionBitsMask;

            enumerator.RemainingBits -= _bitsPerFunction;
            enumerator.NextBits >>= _bitsPerFunction;
            return (ulong)result;

        }

        private static void RefillHashEnumerator(ref HashBitEnumerator enumerator)
        {
            enumerator.RemainingBits = 128;
            var next = XxHash128.HashToUInt128(MemoryMarshal.AsBytes(new ReadOnlySpan<UInt128>(in enumerator.HashData)));
            enumerator.NextBits = next;
            enumerator.HashData = next;
        }

        private static HashBitEnumerator EnumerateHashBits(UInt128 initialHash)
        {
            return new HashBitEnumerator
            {
                HashData = initialHash,
                NextBits = initialHash,
                RemainingBits = 128,
            };

        }
        private struct HashBitEnumerator
        {
            public UInt128 HashData;
            public UInt128 NextBits;
            public int RemainingBits;
        }

        private static UInt128 GetHash(in T value) => System.IO.Hashing.XxHash128.HashToUInt128(MemoryMarshal.AsBytes<T>(new ReadOnlySpan<T>(in value)));

        public override string ToString()
        {
            return $"SizeInBytes = {SizeInBytes}, HashFunctions = {HashFunctions}";
        }

        public void UnionWith(IEnumerable<ReadOnlyMemory<ulong>> enumerable)
        {
            int index = 0;
            foreach (var chunk in enumerable)
            {
                foreach (var bits in chunk.Span)
                {
                    Array[index] |= bits;
                    index++;
                }
            }
        }

        public object GetCounters()
        {
            return new
            {
                DefinitelyNotExistsRatio = RuleOutCounter.HitRatio,
                SizeInBytes
            };
        }
    }

    public record struct ProbabilisticSetParameters(long SizeInBytes, int HashFunctions)
    {
        public override string ToString() => GetBitsPerFunction((ulong)SizeInBytes * 8) + "-" + HashFunctions;
        public static int GetBitsPerFunction(ulong arrayLengthInBits) => BitOperations.Log2(arrayLengthInBits);
    }
}

