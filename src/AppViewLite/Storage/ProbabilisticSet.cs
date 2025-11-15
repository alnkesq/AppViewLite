using System;
using System.Collections.Generic;
using System.IO.Hashing;
using System.Linq;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace AppViewLite
{
    public abstract class ProbabilisticSet
    {
        public HitMissCounter RuleOutCounter = new HitMissCounter();
        public string? ConfigurationParameterName;
    }
    public class ProbabilisticSet<T> : ProbabilisticSet where T : unmanaged
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
        public long SizeInBits => (long)Array.Length * BITS_PER_WORD;
        public int BitsPerFunction => _bitsPerFunction;
        public int HashFunctions => _hashFunctions;

        public ProbabilisticSetParameters Parameters => new(SizeInBytes, HashFunctions, ConfigurationParameterName);
        
        public ProbabilisticSet(ProbabilisticSetParameters parameters)
            : this(parameters.SizeInBytes, parameters.HashFunctions)
        {
            this.ConfigurationParameterName = parameters.FromConfigurationParameter;
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
                ConfigurationParameterName,
                DefinitelyNotExistsRatio = RuleOutCounter.HitRatio,
                LastComputedDefinitelyNotExistsRatio,
                CurrentConfiguration = Parameters.SizeInMegabytes + "@" + Parameters.HashFunctions,
                LastEstimatedItemCount
            };
        }

        public long LastEstimatedItemCount;
        private double LastComputedDefinitelyNotExistsRatio;

        public void CheckProbabilisticSetHealthThreadSafe(ProbabilisticSetHealthCheckContext context)
        {
            var desiredDefinitelyNotExistsRatio = context.MinDesiredDefinitelyNotExistsRatio;


            var estimatedInsertions = EstimatedItemCount;
            LastEstimatedItemCount = estimatedInsertions;
            estimatedInsertions = Math.Max(1024, estimatedInsertions);
            

            
            var currentParameters = this.Parameters;
            var currentComputedDefinitelyNotExistsRatio = currentParameters.GetDefinitelyNotExistRatioEstimation(estimatedInsertions);
            LastComputedDefinitelyNotExistsRatio = currentComputedDefinitelyNotExistsRatio;
            if (currentComputedDefinitelyNotExistsRatio >= desiredDefinitelyNotExistsRatio) return;

            var n = estimatedInsertions * 1.5;
            var p = 1 - desiredDefinitelyNotExistsRatio;

            var m = (ulong)Math.Ceiling((n * Math.Log(p)) / Math.Log(1 / Math.Pow(2, Math.Log(2))));

            var k = (int)Math.Round((m / n) * Math.Log(2));
            m = BitOperations.RoundUpToPowerOf2(m);

            var recommendedParameters = new ProbabilisticSetParameters((long)(m / 8), k);
            var scenarioDefinitelyNotExistsRatio = recommendedParameters.GetDefinitelyNotExistRatioEstimation((long)n);

            lock (context.Problems)
            {
                context.Problems.Add($"Probabilistic cache should be increased for best performance. Consider setting {ConfigurationParameterName}={recommendedParameters.SizeInMegabytes}@{recommendedParameters.HashFunctions}"); //. (DefinitelyNotExistsRatio={RuleOutCounter.HitRatio:0.00}, EstimatedItemCount={EstimatedItemCount})");

                if (scenarioDefinitelyNotExistsRatio < desiredDefinitelyNotExistsRatio)
                {
                    context.Problems.Add($"Minor bug: recommended parameters won't produce desired DefinitelyNotExistsRatio for {ConfigurationParameterName} (EstimatedItemCount={EstimatedItemCount}, scenarioDefinitelyNotExistsRatio={scenarioDefinitelyNotExistsRatio})");
                }
            }

            //var recommendedSizeInBytes = this.SizeInBytes;
            //var n = (long)(estimatedInsertions * 1.5);
            //while (true)
            //{
            //    recommendedSizeInBytes *= 2;
            //    recommendedSizeInBytes = Math.Max(recommendedSizeInBytes, 1024 * 1024);

            //    var m = recommendedSizeInBytes * 8;
            //    var k = (int)Math.Round(m / n * Math.Log(2));
            //    k = Math.Min(25, k);

            //    var recommendedParameters = new ProbabilisticSetParameters(recommendedSizeInBytes, k);
            //    var scenarioDefinitelyNotExistsRatio = recommendedParameters.GetDefinitelyNotExistRatioEstimation((long)n);

            //    if (scenarioDefinitelyNotExistsRatio >= desiredDefinitelyNotExistsRatio)
            //    {
            //        context.Problems.Add($"Probabilistic cache should be increased for best performance. Consider setting {ConfigurationParameterName}={recommendedParameters.SizeInMegabytes}@{recommendedParameters.HashFunctions}"); //. (DefinitelyNotExistsRatio={RuleOutCounter.HitRatio:0.00}, EstimatedItemCount={EstimatedItemCount})");
            //        break;
            //    }
            //}



        }

        public long BitsSetTo1
        {
            get
            {
                long count = 0;
                foreach (var word in this.Array)
                {
                    count += BitOperations.PopCount(word);
                }
                return count;
            }
        }

        public long EstimatedItemCount
        {
            get
            {
                var bitsSetTo1 = BitsSetTo1;
                var fractionOf1 = (double)bitsSetTo1 / SizeInBits;
                var result = (Math.Log(1 - fractionOf1)) / (HashFunctions * Math.Log(1 - 1.0 / SizeInBits));
                return (long)result;
            }
        }
    }

    public record struct ProbabilisticSetParameters(long SizeInBytes, int HashFunctions, string? FromConfigurationParameter = null)
    {
        public override string ToString() => GetBitsPerFunction((ulong)SizeInBytes * 8) + "-" + HashFunctions;
        public static int GetBitsPerFunction(ulong arrayLengthInBits) => BitOperations.Log2(arrayLengthInBits);

        public int SizeInMegabytes = (int)(SizeInBytes / (1024 * 1024));
        public long SizeInBits => SizeInBytes * 8;
        public double GetDefinitelyNotExistRatioEstimation(long itemCount)
        {
            return 1 - Math.Pow(1 - Math.Exp(-HashFunctions / ((double)SizeInBits / itemCount)), HashFunctions);
        }
    }

    public class ProbabilisticSetHealthCheckContext
    {
        public double MinDesiredDefinitelyNotExistsRatio;
        [JsonInclude] public readonly DateTime CreatedAt = DateTime.UtcNow;
        [JsonInclude] public readonly List<string> Problems = [];
    }
}

