using System.Text.Json;
using System.Text.Json.Serialization;

namespace FastTransfer.Shared;

/// <summary>FAST event names. Each has its own endpoint (POST /events/{event}) and Kafka topic.</summary>
public static class FastEvents
{
    public const string Send = "fast_send";
    public const string Success = "fast_send_success";
    public const string Failed = "fast_send_failed";

    public static readonly string[] All = [Send, Success, Failed];

    public static bool IsKnown(string name) => Array.IndexOf(All, name) >= 0;
}

/// <summary>FAST transfer payload accepted by the endpoints (same JSON shape as the Go service).</summary>
public sealed class Transfer
{
    public string TransactionId { get; set; } = "";
    public decimal Amount { get; set; }
    public string Currency { get; set; } = "";
    public string? SenderIban { get; set; }
    public string? ReceiverIban { get; set; }
    public string? SenderBank { get; set; }
    public string? ReceiverBank { get; set; }
    public string? Description { get; set; }
    public string? FailureReason { get; set; }
    public string? SentAt { get; set; }
    public string? Source { get; set; }
}

/// <summary>Envelope written to Kafka.</summary>
public sealed class FastEvent
{
    public string Id { get; set; } = "";
    public string Event { get; set; } = "";
    public string TransactionId { get; set; } = "";
    public Transfer Payload { get; set; } = new();
    public string CreatedAt { get; set; } = "";
}

public static class Json
{
    /// <summary>snake_case, omit nulls — matches the Go service's JSON.</summary>
    public static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };
}

public static class Env
{
    public static string Get(string key, string fallback) =>
        Environment.GetEnvironmentVariable(key) is { Length: > 0 } v ? v : fallback;

    /// <summary>Go-style durations: "30s", "10m", "500ms", "1h".</summary>
    public static TimeSpan Duration(string key, TimeSpan fallback)
    {
        var raw = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(raw)) return fallback;
        raw = raw.Trim();
        (string unit, double factor)[] units = [("ms", 1), ("s", 1000), ("m", 60_000), ("h", 3_600_000)];
        foreach (var (unit, factor) in units)
        {
            if (raw.EndsWith(unit, StringComparison.Ordinal) &&
                double.TryParse(raw[..^unit.Length], System.Globalization.CultureInfo.InvariantCulture, out var n) &&
                !(unit == "s" && raw.EndsWith("ms", StringComparison.Ordinal)))
            {
                return TimeSpan.FromMilliseconds(n * factor);
            }
        }
        Console.WriteLine($"invalid {key}={raw}, using {fallback}");
        return fallback;
    }
}
