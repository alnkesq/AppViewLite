namespace AppViewLite.Models
{
    public record BlobToUpload(string ContentType, string? AltText, byte[] UploadedBytes)
    {
    }
}

