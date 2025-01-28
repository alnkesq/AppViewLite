using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Storage
{
    public static class CompactStructCheck<T> where T : struct
    {
        private static int _size = -1;
        public static int Check()
        {
            if (_size == -1)
            {
                _size = CheckCore();
            }

            return _size;
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
                    var fieldSize = (int)typeof(CompactStructCheck<>).MakeGenericType(field.FieldType).GetMethod(nameof(Check), BindingFlags.Static | BindingFlags.Public)!.Invoke(null, null)!;
                    computedSum += fieldSize;
                }
                var inlineArray = typeof(T).GetCustomAttribute<InlineArrayAttribute>();
                if (inlineArray != null)
                    computedSum *= inlineArray.Length;
                if (computedSum != size && typeof(T).GetCustomAttribute<StructLayoutAttribute>() == null)
                    throw new Exception($"Missing [StructLayout(LayoutKind.Sequential, Pack = 1)] attribute for {typeof(T).FullName}. This can lead to wasted storage space.");
                
            }
            return size;
        }
    }
}

