using System.Collections.Concurrent;
using System.Text.Json;
using Confluent.Kafka;
using Prometheus;

namespace MockConsumer;

/// <summary>
/// Health of the subscription loops, reported by /healthz (503 when unhealthy) and by the
/// consumer_subscription_up gauge. A subscription is healthy while its consume loop runs and at least
/// one Kafka broker has been reachable within <see cref="BrokerGrace"/>. Broker reachability comes from
/// librdkafka statistics (every few seconds, also when no messages arrive), so an idle consumer is not
/// reported as unhealthy and recovery is detected without traffic. A Local_AllBrokersDown error marks
/// Kafka unreachable at once (statistics can still show a hung connection as UP until it times out).
/// </summary>
public sealed class ConsumerHealth
{
    public static readonly TimeSpan BrokerGrace = TimeSpan.FromSeconds(30);

    static readonly Gauge Up = Metrics.CreateGauge("consumer_subscription_up",
        "1 while the subscription's consume loop runs and Kafka is reachable, by consumer group.", "consumergroup");

    readonly ConcurrentDictionary<string, State> _states = new();
    readonly TimeProvider _time;

    public ConsumerHealth(TimeProvider? time = null) => _time = time ?? TimeProvider.System;

    sealed class State
    {
        public volatile bool Running;
        public volatile string? StopReason;
        public volatile string? LastError;
        public volatile bool AllBrokersDown;
        public long LastBrokerUpTicks;
    }

    State Get(string group) => _states.GetOrAdd(group, _ => new State());

    /// <summary>The loop subscribed; the broker grace period starts now.</summary>
    public void Started(string group)
    {
        var s = Get(group);
        Interlocked.Exchange(ref s.LastBrokerUpTicks, _time.GetUtcNow().UtcTicks);
        s.StopReason = null;
        s.Running = true;
        Publish(group, s);
    }

    /// <summary>The loop ended for a reason other than shutdown.</summary>
    public void Stopped(string group, string reason)
    {
        var s = Get(group);
        s.Running = false;
        s.StopReason = reason;
        Publish(group, s);
    }

    public void KafkaError(string group, Error error)
    {
        var s = Get(group);
        s.LastError = $"{error.Code}: {error.Reason}";
        if (error.Code == ErrorCode.Local_AllBrokersDown)
        {
            s.AllBrokersDown = true;
        }
        if (error.IsFatal)
        {
            s.Running = false;
            s.StopReason = $"fatal Kafka error {error.Code}";
        }
        Publish(group, s);
    }

    /// <summary>librdkafka statistics: a broker with a node id (not a bootstrap entry) in state UP.</summary>
    public void Statistics(string group, string json)
    {
        var s = Get(group);
        try
        {
            using var doc = JsonDocument.Parse(json);
            if (doc.RootElement.TryGetProperty("brokers", out var brokers) &&
                brokers.EnumerateObject().Any(b =>
                    b.Value.TryGetProperty("nodeid", out var id) && id.GetInt32() >= 0 &&
                    b.Value.TryGetProperty("state", out var state) && state.GetString() == "UP"))
            {
                Interlocked.Exchange(ref s.LastBrokerUpTicks, _time.GetUtcNow().UtcTicks);
                s.AllBrokersDown = false;
            }
        }
        catch (JsonException)
        {
            // Malformed statistics: keep the previous value; the grace period decides.
        }
        Publish(group, s);
    }

    public HealthReport Check()
    {
        var now = _time.GetUtcNow();
        var subscriptions = _states
            .OrderBy(kv => kv.Key, StringComparer.Ordinal)
            .Select(kv => Describe(kv.Key, kv.Value, now))
            .ToArray();
        var healthy = subscriptions.Length > 0 && subscriptions.All(s => s.Healthy);
        return new HealthReport(healthy ? "ok" : "unhealthy", subscriptions);
    }

    static SubscriptionHealth Describe(string group, State s, DateTimeOffset now)
    {
        var lastUp = new DateTimeOffset(Interlocked.Read(ref s.LastBrokerUpTicks), TimeSpan.Zero);
        var reachable = !s.AllBrokersDown && now - lastUp <= BrokerGrace;
        return new SubscriptionHealth(group, s.Running && reachable, s.Running, reachable,
            lastUp.ToString("O"), s.StopReason, s.LastError);
    }

    void Publish(string group, State s) =>
        Up.WithLabels(group).Set(Describe(group, s, _time.GetUtcNow()).Healthy ? 1 : 0);
}

public sealed record HealthReport(string Status, SubscriptionHealth[] Subscriptions);

public sealed record SubscriptionHealth(
    string ConsumerGroup, bool Healthy, bool Running, bool KafkaReachable,
    string LastBrokerUpAt, string? StopReason, string? LastError);
