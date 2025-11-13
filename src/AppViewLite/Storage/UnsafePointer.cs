using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace AppViewLite.Storage
{
    public readonly unsafe struct UnsafePointer<T> where T : unmanaged
    {
        public readonly T* Pointer;
        public UnsafePointer(T* ptr)
        {
            this.Pointer = ptr;
        }
        public static implicit operator UnsafePointer<T>(T* ptr) => new UnsafePointer<T>(ptr);
        public static implicit operator T*(UnsafePointer<T> p) => p.Pointer;

        public ref T AsRef => ref Unsafe.AsRef<T>(Pointer);
    }
}

