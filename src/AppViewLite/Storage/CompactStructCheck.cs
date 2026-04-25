using System;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace AppViewLite.Storage
{
    public static class CompactStructCheck
    {
        internal const int SizeNotComputed = -1;
        internal const int SizeWasteful = -2;

        public static bool IsWasteful<T>() where T : struct, allows ref struct
        {
            return CompactStructCheck<T>.GetSizeOrWasteful() == SizeWasteful;
        }
        public static void Check<T>() where T : struct, allows ref struct
        {
            CompactStructCheck<T>.Check();
        }

        public static bool IsWasteful(Type t) => (bool)typeof(CompactStructCheck).GetMethod(nameof(IsWasteful), BindingFlags.Public | BindingFlags.Static, [])!.MakeGenericMethod([t]).Invoke(null, null)!;
        public static void Check(Type t) => typeof(CompactStructCheck).GetMethod(nameof(Check), BindingFlags.Public | BindingFlags.Static, [])!.MakeGenericMethod([t]).Invoke(null, null);

        public static void PrintWastefulTypes(Assembly assembly)
        {
            foreach (var type in assembly.GetTypes())
            {
                if (type.IsValueType && !type.ContainsGenericParameters && type.FullName?.StartsWith("<PrivateImplementationDetails>", StringComparison.Ordinal) != true && !Attribute.IsDefined(type, typeof(CompilerGeneratedAttribute)))
                {
                    if (IsWasteful(type))
                    {
                        Console.WriteLine(type.FullName);
                    }
                }
            }
        }
    }
    internal static class CompactStructCheck<T> where T : struct, allows ref struct
    {
        private static int _size = CompactStructCheck.SizeNotComputed;
        public static int GetSizeOrWasteful()
        {
            if (_size == CompactStructCheck.SizeNotComputed)
            {
                _size = CheckCore();
            }

            return _size;
        }
        public static void Check()
        {
            var size = GetSizeOrWasteful();
            if (size == CompactStructCheck.SizeWasteful)
            {
                throw new Exception($"Missing [StructLayout(LayoutKind.Sequential, Pack = 1)] attribute for {typeof(T).FullName}. This can lead to wasted storage space.");
            }
        }
        private static int CheckCore()
        {
            var size = Unsafe.SizeOf<T>();
            if (typeof(T).Assembly != typeof(int).Assembly)
            {
                var fields = typeof(T).GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                var computedSum = 0;
                foreach (var field in fields)
                {
                    int fieldSize;
                    var fieldType = field.FieldType;
                    if (fieldType.IsValueType)
                    {
                        var nonNullableType = Nullable.GetUnderlyingType(fieldType) ?? fieldType;

                        fieldSize = (int)typeof(CompactStructCheck<>).MakeGenericType(nonNullableType).GetMethod(nameof(GetSizeOrWasteful), BindingFlags.Static | BindingFlags.Public)!.Invoke(null, null)!;
                        if (nonNullableType != fieldType && fieldSize != CompactStructCheck.SizeWasteful)
                            fieldSize += Marshal.SizeOf<bool>();

                    }
                    else
                    {
                        fieldSize = Marshal.SizeOf<IntPtr>();
                    }
                    
                    if(fieldSize == CompactStructCheck.SizeWasteful) return CompactStructCheck.SizeWasteful;
                    computedSum += fieldSize;
                }
                var inlineArray = typeof(T).GetCustomAttribute<InlineArrayAttribute>();
                if (inlineArray != null)
                    computedSum *= inlineArray.Length;
                if (computedSum != size && !Attribute.IsDefined(typeof(T), typeof(StructLayoutAttribute)))
                    return CompactStructCheck.SizeWasteful;
            }
            return size;
        }
    }
}

