using System.Text.Json;
using System.Text.Json.Serialization;

namespace Shared;

/// <summary>Transfer event names. Each has its own endpoint and Kafka topic.</summary>
public static class TransferEvents
{
    public const string FastIncomingRequest = "fast_incoming_request";
    public const string FastIncomingSuccess = "fast_incoming_success";
    public const string FastIncomingFailed = "fast_incoming_failed";
    public const string FastOutgoingSuccess = "fast_outgoing_success";
    public const string FastOutgoingFailed = "fast_outgoing_failed";
    public const string FastOutgoingRequest = "fast_outgoing_request";
    public const string HavaleRequest = "havale_request";
    public const string HavaleSuccess = "havale_success";
    public const string HavaleFailed = "havale_failed";
    public const string EftIncomingSuccess = "eft_incoming_success";
    public const string EftIncomingFailed = "eft_incoming_failed";
    public const string EftOutgoingSuccess = "eft_outgoing_success";
    public const string EftOutgoingFailed = "eft_outgoing_failed";
    public const string EftIncomingRequest = "eft_incoming_request";
    public const string EftOutgoingRequest = "eft_outgoing_request";

    public static readonly string[] All =
    [
        FastIncomingRequest,
        FastIncomingSuccess, FastIncomingFailed,
        FastOutgoingRequest,
        FastOutgoingSuccess, FastOutgoingFailed,
        HavaleRequest,
        HavaleSuccess, HavaleFailed,
        EftIncomingRequest,
        EftIncomingSuccess, EftIncomingFailed,
        EftOutgoingRequest,
        EftOutgoingSuccess, EftOutgoingFailed,
    ];

    public static string EndpointPath(string eventName) => eventName switch
    {
        FastIncomingRequest => "/events/fast/incoming/request",
        FastIncomingSuccess => "/events/fast/incoming/success",
        FastIncomingFailed => "/events/fast/incoming/failed",
        FastOutgoingSuccess => "/events/fast/outgoing/success",
        FastOutgoingFailed => "/events/fast/outgoing/failed",
        FastOutgoingRequest => "/events/fast/outgoing/request",
        HavaleRequest => "/events/havale/request",
        HavaleSuccess => "/events/havale/success",
        HavaleFailed => "/events/havale/failed",
        EftIncomingSuccess => "/events/eft/incoming/success",
        EftIncomingFailed => "/events/eft/incoming/failed",
        EftIncomingRequest => "/events/eft/incoming/request",
        EftOutgoingSuccess => "/events/eft/outgoing/success",
        EftOutgoingFailed => "/events/eft/outgoing/failed",
        EftOutgoingRequest => "/events/eft/outgoing/request",
        _ => $"/events/{eventName}",
    };

    public static bool TryGetEventName(string path, out string eventName)
    {
        eventName = path.Trim('/').ToLowerInvariant() switch
        {
            "events/fast/incoming/request" => FastIncomingRequest,
            "events/fast/incoming/success" => FastIncomingSuccess,
            "events/fast/incoming/failed" => FastIncomingFailed,
            "events/fast/outgoing/success" => FastOutgoingSuccess,
            "events/fast/outgoing/failed" => FastOutgoingFailed,
            "events/fast/outgoing/request" => FastOutgoingRequest,
            "events/havale/request" => HavaleRequest,
            "events/havale/success" => HavaleSuccess,
            "events/havale/failed" => HavaleFailed,
            "events/eft/incoming/success" => EftIncomingSuccess,
            "events/eft/incoming/failed" => EftIncomingFailed,
            "events/eft/incoming/request" => EftIncomingRequest,
            "events/eft/outgoing/success" => EftOutgoingSuccess,
            "events/eft/outgoing/failed" => EftOutgoingFailed,
            "events/eft/outgoing/request" => EftOutgoingRequest,
            _ => "",
        };
        return eventName.Length > 0;
    }

    public static bool IsKnown(string name) => Array.IndexOf(All, name) >= 0;
}

/// <summary>FAST transfer payload accepted by the endpoints (snake_case JSON).</summary>
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
public sealed class TransferEvent
{
    /// <summary>Event id: a UUIDv7, unique across producer replicas and ordered by creation time.</summary>
    public string Id { get; set; } = "";
    public string Event { get; set; } = "";
    public string TransactionId { get; set; } = "";
    public Transfer Payload { get; set; } = new();
    public string CreatedAt { get; set; } = "";
}

public static class Json
{
    /// <summary>snake_case, omit nulls.</summary>
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

    /// <summary>Durations such as "30s", "10m", "500ms", "1h".</summary>
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
