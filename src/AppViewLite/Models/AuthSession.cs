using System.Text.Json;
using System.Text.Json.Serialization;

namespace AppViewLite;

public class AuthSession
{

    [JsonPropertyName("session")]
    public required Session Session { get; set; }

    [JsonPropertyName("proofKey")]
    public string ProofKey { get; set; } = "";

    public static AuthSession? FromString(string json)
    {
        return JsonSerializer.Deserialize<AuthSession>(json);
    }
    public override string ToString()
    {
        return JsonSerializer.Serialize<AuthSession>(this);
    }
}
