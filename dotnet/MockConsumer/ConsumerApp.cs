using System.Text.Json;
using Confluent.Kafka;
using Shared;
using Prometheus;

namespace MockConsumer;

/// <summary>
/// Consumes the configured transfer topics with the configured consumer group and exposes /metrics and /healthz.
/// Several replicas can run in the same group; Prometheus scrapes each replica separately.
///
/// This service only logs each event and reports technical metrics
/// (throughput, errors, latency and partitions). Business reconciliation remains outside this mock service.
/// </summary>
public static class ConsumerApp
{
    public static WebApplication Build(string[] args)
    {
        var builder = WebApplication.CreateSlimBuilder(args);
        builder.WebHost.UseUrls(Env.Get("METRICS_ADDR", "http://0.0.0.0:2114"));

        var settings = new ConsumerSettings(
            Broker: Env.Get("KAFKA_BROKERS", "kafka:9092"),
            Subscriptions: ParseSubscriptions());
        builder.Services.AddSingleton(settings);
        builder.Services.AddSingleton(new ConsumerHealth());
        builder.Services.AddHostedService<ConsumerWorker>();

        var app = builder.Build();
        // 200 while every subscription loop runs and Kafka is reachable, 503 otherwise (JSON details).
        app.MapGet("/healthz", (ConsumerHealth health) =>
        {
            var report = health.Check();
            return Results.Json(report, Json.Options,
                statusCode: report.Status == "ok" ? StatusCodes.Status200OK : StatusCodes.Status503ServiceUnavailable);
        });
        app.MapMetrics("/metrics");
        return app;
    }

    static ConsumerSubscription[] ParseSubscriptions()
    {
        var raw = Env.Get("KAFKA_SUBSCRIPTIONS", "fast_incoming=fast_incoming_request,fast_incoming_success,fast_incoming_failed");
        return raw.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(entry =>
            {
                var parts = entry.Split('=', 2, StringSplitOptions.TrimEntries);
                if (parts.Length != 2 || string.IsNullOrWhiteSpace(parts[0]) || string.IsNullOrWhiteSpace(parts[1]))
                    throw new InvalidOperationException($"Invalid KAFKA_SUBSCRIPTIONS entry: {entry}");
                return new ConsumerSubscription(
                    parts[0],
                    parts[1].Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries));
            })
            .ToArray();
    }
}

public sealed record ConsumerSettings(string Broker, ConsumerSubscription[] Subscriptions);
public sealed record ConsumerSubscription(string Group, string[] Topics);

public sealed class ConsumerWorker(ConsumerSettings settings, ConsumerHealth health, ILogger<ConsumerWorker> log) : BackgroundService
{
    static readonly Counter Consumed = Metrics.CreateCounter("consumer_events_total",
        "Transfer events successfully consumed from Kafka, by HTTP endpoint.", "endpoint");
    static readonly Counter Errors = Metrics.CreateCounter("consumer_errors_total", "Consumer errors, by endpoint and reason.", "endpoint", "reason");
    static readonly Counter ByPartition = Metrics.CreateCounter("consumer_partition_events_total",
        "Events consumed, by consumer group, endpoint, topic and partition.", "consumergroup", "endpoint", "topic", "partition");
    static readonly Gauge LastOffset = Metrics.CreateGauge("consumer_last_offset",
        "Offset of the last message consumed, by consumer group, endpoint, topic and partition.", "consumergroup", "endpoint", "topic", "partition");
    static readonly Histogram Latency = Metrics.CreateHistogram("consumer_event_latency_seconds",
        "Time between Kafka write and consumption, by HTTP endpoint.",
        new HistogramConfiguration { LabelNames = ["endpoint"], Buckets = Histogram.ExponentialBuckets(0.005, 2, 16) });

    // Confluent's Consume() blocks, so the loop runs on its own thread instead of the thread pool.
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.WhenAll(settings.Subscriptions.Select(subscription =>
            Task.Factory.StartNew(() => Run(subscription, stoppingToken), stoppingToken,
                TaskCreationOptions.LongRunning, TaskScheduler.Default)));

    void Run(ConsumerSubscription subscription, CancellationToken ct)
    {
        foreach (var t in subscription.Topics)
        {
            Consumed.WithLabels(TransferEvents.EndpointPath(t));
        }

        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = settings.Broker,
            GroupId = subscription.Group,
            ClientId = $"mock-consumer-dotnet-{subscription.Group}",
            AutoOffsetReset = AutoOffsetReset.Earliest,  // a new group starts from the beginning
            EnableAutoCommit = true,
            AutoCommitIntervalMs = 1000,                  // commit offsets every second
            EnableAutoOffsetStore = true,
            // No cross-event state here, so any assignment works; cooperative-sticky rebalances incrementally
            // (replicas keep their other partitions while one moves) when replicas are added or removed.
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            // Statistics carry the broker states that ConsumerHealth uses (also while no messages arrive).
            StatisticsIntervalMs = 5000,
        })
            .SetPartitionsAssignedHandler((_, partitions) =>
                log.LogInformation("partitions assigned: {Partitions}", string.Join(", ", partitions.Select(p => $"{p.Topic}[{p.Partition.Value}]"))))
            .SetErrorHandler((_, e) =>
            {
                Errors.WithLabels("unknown", "read").Inc();
                health.KafkaError(subscription.Group, e);
                log.LogWarning("kafka error: {Reason}", e.Reason);
            })
            .SetStatisticsHandler((_, json) => health.Statistics(subscription.Group, json))
            .Build();

        consumer.Subscribe(subscription.Topics);
        health.Started(subscription.Group);
        log.LogInformation("consumer started; kafka={Broker} topics={Topics} group={Group} instance={Instance}",
            settings.Broker, string.Join(",", subscription.Topics), subscription.Group, Environment.MachineName);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                ConsumeResult<string, string>? result;
                try
                {
                    result = consumer.Consume(ct);
                }
                catch (ConsumeException ex)
                {
                    Errors.WithLabels("unknown", "read").Inc();
                    log.LogWarning("read error: {Error}", ex.Error.Reason);
                    continue;
                }
                if (result?.Message is null) continue;
                try
                {
                    Handle(result, subscription.Group);
                }
                catch (Exception ex)
                {
                    // One bad message must not end this subscription's loop (the thread would stop
                    // silently while /healthz keeps answering "ok").
                    Errors.WithLabels(TransferEvents.EndpointPath(result.Topic), "handle").Inc();
                    log.LogError(ex, "handle error topic={Topic} partition={Partition} offset={Offset}",
                        result.Topic, result.Partition.Value, result.Offset.Value);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // shutting down
        }
        catch (Exception ex)
        {
            // The loop cannot continue: report it through /healthz and consumer_subscription_up.
            health.Stopped(subscription.Group, ex.Message);
            log.LogError(ex, "consume loop for group {Group} stopped", subscription.Group);
        }
        finally
        {
            try
            {
                consumer.Close(); // commits offsets and leaves the group cleanly
            }
            catch (KafkaException ex)
            {
                log.LogWarning("close failed for group {Group}: {Error}", subscription.Group, ex.Error.Reason);
            }
        }
    }

    void Handle(ConsumeResult<string, string> result, string group)
    {
        TransferEvent? evt;
        try
        {
            evt = JsonSerializer.Deserialize<TransferEvent>(result.Message.Value, Json.Options);
        }
        catch (JsonException ex)
        {
            evt = null;
            log.LogWarning("decode error topic={Topic} offset={Offset}: {Error}", result.Topic, result.Offset.Value, ex.Message);
        }
        // "payload": null deserializes to a null Payload despite the non-nullable property type.
        if (evt?.Payload is null)
        {
            Errors.WithLabels(TransferEvents.EndpointPath(result.Topic), "decode").Inc();
            return;
        }

        var name = string.IsNullOrEmpty(evt.Event) ? result.Topic : evt.Event;
        var endpoint = TransferEvents.EndpointPath(name);
        var amount = (double)evt.Payload.Amount;

        // Consumers intentionally only log and expose technical metrics.
        Consumed.WithLabels(endpoint).Inc();
        if (result.Message.Timestamp.Type != TimestampType.NotAvailable)
        {
            Latency.WithLabels(endpoint).Observe((DateTime.UtcNow - result.Message.Timestamp.UtcDateTime).TotalSeconds);
        }

        var partition = result.Partition.Value.ToString();
        ByPartition.WithLabels(group, endpoint, result.Topic, partition).Inc();
        LastOffset.WithLabels(group, endpoint, result.Topic, partition).Set(result.Offset.Value);
        // Per-message Information logging adds synchronous console I/O under load. The counters
        // and latency histogram remain available for observability; enable Debug when samples are needed.
        log.LogDebug("consumed event={Event} transaction_id={TransactionId} amount={Amount:F2} TRY partition={Partition} offset={Offset}",
            name, evt.TransactionId, amount, result.Partition.Value, result.Offset.Value);
    }
}
