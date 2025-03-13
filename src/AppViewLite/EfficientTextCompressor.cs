using Microsoft.Extensions.ObjectPool;
using Microsoft.ML.Tokenizers;
using Sewer56.BitStream;
using Sewer56.BitStream.ByteStreams;
using System;
using System.Collections.Generic;

namespace AppViewLite
{
    public static class EfficientTextCompressor
    {
        // Wrapper, because ObjectPool<T> needs T with public parameterless ctor.
        internal class TiktokenWrapper
        {
            public readonly TiktokenTokenizer Tokenizer;
            public TiktokenWrapper()
            { 
                this.Tokenizer = TiktokenTokenizer.CreateForModel("gpt-4o"); 
            }
        }



        private readonly static ObjectPool<TiktokenWrapper> TokenizerPool = ObjectPool.Create<TiktokenWrapper>();


        public const int MaxLength = 1024 * 4;
        private static IReadOnlyList<int> BpeCompress(string text)
        {
            if (text.Length > MaxLength) throw new UnexpectedFirehoseDataException("Exceeded maximum text size for BPE compression.");
            var tokenizer = TokenizerPool.Get();
            var result = tokenizer.Tokenizer.EncodeToIds(text, int.MaxValue, out var normalized, out var consumed, considerPreTokenization: false, considerNormalization: false);
            TokenizerPool.Return(tokenizer);
            return result;
        }
        private static string BpeDecompress(IReadOnlyList<int> bpe)
        {
            var tokenizer = TokenizerPool.Get();
            var t = tokenizer.Tokenizer.Decode(bpe);
            TokenizerPool.Return(tokenizer);
            return t;
        }

        public static void DecompressInPlace(ref string? text, ref byte[]? compressed)
        {
            if (compressed == null) return;
            text = Decompress(compressed);
            compressed = null;
        }

        public static void CompressInPlace(ref string? text, ref byte[]? compressed)
        {
            if (string.IsNullOrEmpty(text))
            {
                compressed = null;
                text = null;
                return;
            }
            compressed = Compress(text);
            text = null;
        }

        public static byte[]? Compress(string? text)
        {
            if (string.IsNullOrEmpty(text)) return null;
            var tokens = BpeCompress(text);
            var result = CompressBitStream(tokens);

            //var roundtrip = Decompress(result);
            //if (roundtrip != text) throw new Exception("Roundtripping error");
            return result;
        }

        public const int Step1 = 11;
        public const int Step2 = 14;
        public const int Step3 = 0;
        public const int MaxBitSize = 18;


        private readonly static int BitStreamStepCount =
                Step1 == 0 ? 0 :
                Step2 == 0 ? 1 :
                Step3 == 0 ? 2 :
                3;

        public static IReadOnlyList<int> DecompressBitStream(byte[] bytes)
        {
            var bitstream = new BitStream<ArrayByteStream>(new ArrayByteStream(bytes));

            var stepCount = BitStreamStepCount;
            if (stepCount != 2) throw new NotImplementedException();

            var result = new List<int>();
            while (true)
            {
                var remaining = bytes.Length * 8 - bitstream.BitIndex;
                if (remaining == 0) break;
                uint num;
                if (bitstream.ReadBit() == 0)
                {
                    remaining--;
                    if(remaining < Step1)
                    {
                        if (remaining == 0) break;
                        var rest = bitstream.Read32(remaining);
                        if (rest != 0) throw new Exception();
                        break;
                    }
                    num = bitstream.Read32(Step1);
                }
                else
                {
                    if (bitstream.ReadBit() == 0)
                    {
                        num = bitstream.Read32(Step2);
                    }
                    else
                    {
                        num = bitstream.Read32(MaxBitSize);
                    }
                }
                result.Add((int)num);
            }

            return result;

        }

        public static byte[] CompressBitStream(IReadOnlyList<int> tokens)
        {
            var buffer = new byte[tokens.Count * 4];
            var bitstream = new BitStream<ArrayByteStream>(new ArrayByteStream(buffer));

            void WriteChecked(uint value, int bits)
            {
                if (value >= (1 << bits)) throw new ArgumentException();
                bitstream.Write(value, bits);
            }

            var stepCount = BitStreamStepCount;

            foreach (var token in tokens)
            {
                var t = (uint)token;


                //switch (stepCount)
                //{
                //    case 0:
                //        WriteChecked(t, MaxBitSize);
                //        break;
                //    case 1:
                //        if (token < (1 << Step1))
                //        {
                //            bitstream.WriteBit(0);
                //            WriteChecked(t, Step1);
                //        }
                //        else
                //        {

                //            bitstream.WriteBit(1);
                //            WriteChecked(t, MaxBitSize);

                //        }
                //        break;
                //    case 2:
                        if (token < (1 << Step1))
                        {
                            bitstream.WriteBit(0);
                            WriteChecked(t, Step1);
                        }
                        else
                        {
                            if (token < (1 << Step2))
                            {
                                bitstream.WriteBit(1);
                                bitstream.WriteBit(0);
                                WriteChecked(t, Step2);
                            }
                            else
                            {
                                bitstream.WriteBit(1);
                                bitstream.WriteBit(1);
                                WriteChecked(t, MaxBitSize);
                            }
                        }
                    //    break;
                  
                    //case 3:
                    //    if (token < (1 << Step1))
                    //    {
                    //        bitstream.WriteBit(0);
                    //        WriteChecked(t, Step1);
                    //    }
                    //    else
                    //    {
                    //        if (token < (1 << Step2))
                    //        {
                    //            bitstream.WriteBit(1);
                    //            bitstream.WriteBit(0);
                    //            WriteChecked(t, Step2);
                    //        }
                    //        else
                    //        {

                    //            if (token < (1 << Step3))
                    //            {
                    //                bitstream.WriteBit(1);
                    //                bitstream.WriteBit(1);
                    //                bitstream.WriteBit(0);
                    //                WriteChecked(t, Step3);
                    //            }
                    //            else
                    //            {

                    //                bitstream.WriteBit(1);
                    //                bitstream.WriteBit(1);
                    //                bitstream.WriteBit(1);
                    //                WriteChecked(t, MaxBitSize);
                    //            }
                    //        }
                    //    }

                    //    break;
                //}

            }

            return buffer.AsSpan(0, bitstream.NextByteIndex).ToArray();


        }

        public static string? Decompress(byte[]? compressed)
        {
            if (compressed == null || compressed.Length == 0) return null;
            var bpe = DecompressBitStream(compressed);
            return BpeDecompress(bpe);
        }

    }
}

