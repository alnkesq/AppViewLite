using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.IconParser
{

#pragma warning disable CS0649

    [StructLayout(LayoutKind.Sequential, Pack = 2)]
    internal struct BITMAPFILEHEADER
    {
        public ushort bfType;

        public uint bfSize;

        public ushort bfReserved1;

        public ushort bfReserved2;

        public uint bfOffBits;
    }

    internal struct BITMAPINFOHEADER
    {
        public uint biSize;

        public int biWidth;

        public int biHeight;

        public ushort biPlanes;

        public ushort biBitCount;

        public uint biCompression;

        public uint biSizeImage;

        public int biXPelsPerMeter;

        public int biYPelsPerMeter;

        public uint biClrUsed;

        public uint biClrImportant;
    }
}

