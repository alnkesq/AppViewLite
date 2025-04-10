using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public record BlobToUpload(string ContentType, string? AltText, byte[] UploadedBytes)
    {
    }
}

