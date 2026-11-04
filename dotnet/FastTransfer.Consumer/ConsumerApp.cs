using System.Text.Json;
using Confluent.Kafka;
using FastTransfer.Shared;
using Prometheus;

namespace FastTransfer.Consumer;

/// <summary>
/// Consumes the FAST topics with the configured consumer group and exposes /metrics and /healthz.
/// Several replicas can run in the same group; Prometheus scrapes each replica separately.
///
/// This service only does the per-event business processing and reports technical metrics
/// (throughput, errors, latency, partitions). Matching fast_send with its outcome and the FAST business
/// metrics (completion time, banks, amounts, stuck transfers) live in the Kafka Streams reconciler
/// (reconciler/), whose state survives restarts and scaling.
/// </summary>
public static class ConsumerApp
{
    public static WebApplication Build(string[] args)
    {
        var builder = WebApplication.CreateSlimBuilder(args);
        builder.WebHost.UseUrls(Env.Get("METRICS_ADDR", "http://0.0.0.0:2114"));

        var settings = new ConsumerSettings(
            Broker: Env.Get("KAFKA_BROKERS", "kafka:9092"),
            Topics: Env.Get("KAFKA_TOPICS", string.Join(",", FastEvents.All)).Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries),
            Group: Env.Get("KAFKA_GROUP", "fast-transfer-consumers-dotnet"));
        builder.Services.AddSingleton(settings);
        builder.Services.AddHostedService<ConsumerWorker>();

        var app = builder.Build();
        app.MapGet("/healthz", () => Results.Text("ok"));
        app.MapMetrics("/metrics");
        return app;
    }
}

public sealed record ConsumerSettings(string Broker, string[] Topics, string Group);

public sealed class ConsumerWorker(ConsumerSettings settings, ILogger<ConsumerWorker> log) : BackgroundService
{
    static readonly Counter Consumed = Metrics.CreateCounter("consumer_events_total",
        "FAST events successfully consumed from Kafka, by event.", "event");
    static readonly Counter Errors = Metrics.CreateCounter("consumer_errors_total", "Consumer errors, by reason.", "reason");
    static readonly Counter ByPartition = Metrics.CreateCounter("consumer_partition_events_total",
        "Events consumed, by consumer group, topic and partition.", "consumergroup", "topic", "partition");
    static readonly Gauge LastOffset = Metrics.CreateGauge("consumer_last_offset",
        "Offset of the last message consumed, by consumer group, topic and partition.", "consumergroup", "topic", "partition");
    static readonly Histogram Latency = Metrics.CreateHistogram("consumer_event_latency_seconds",
        "Time between Kafka write and consumption, by event.",
        new HistogramConfiguration { LabelNames = ["event"], Buckets = Histogram.ExponentialBuckets(0.005, 2, 16) });

    // Confluent's Consume() blocks, so the loop runs on its own thread instead of the thread pool.
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.Factory.StartNew(() => Run(stoppingToken), stoppingToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);

    void Run(CancellationToken ct)
    {
        foreach (var t in settings.Topics)
        {
            Consumed.WithLabels(t);
        }

        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = settings.Broker,
            GroupId = settings.Group,
            ClientId = "fast-consumer-dotnet",
            AutoOffsetReset = AutoOffsetReset.Earliest,  // a new group starts from the beginning
            EnableAutoCommit = true,
            AutoCommitIntervalMs = 1000,                  // commit offsets every second
            EnableAutoOffsetStore = true,
            // No cross-event state here, so any assignment works; cooperative-sticky rebalances incrementally
            // (replicas keep their other partitions while one moves) when replicas are added or removed.
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
        })
            .SetPartitionsAssignedHandler((_, partitions) =>
                log.LogInformation("partitions assigned: {Partitions}", string.Join(", ", partitions.Select(p => $"{p.Topic}[{p.Partition.Value}]"))))
            .SetErrorHandler((_, e) =>
            {
                Errors.WithLabels("read").Inc();
                log.LogWarning("kafka error: {Reason}", e.Reason);
            })
            .Build();

        consumer.Subscribe(settings.Topics);
        log.LogInformation("consumer started; kafka={Broker} topics={Topics} group={Group} instance={Instance}",
            settings.Broker, string.Join(",", settings.Topics), settings.Group, Environment.MachineName);

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
                    Errors.WithLabels("read").Inc();
                    log.LogWarning("read error: {Error}", ex.Error.Reason);
                    continue;
                }
                if (result?.Message is null) continue;
                Handle(result);
            }
        }
        catch (OperationCanceledException)
        {
            // shutting down
        }
        finally
        {
            consumer.Close(); // commits offsets and leaves the group cleanly
        }
    }

    void Handle(ConsumeResult<string, string> result)
    {
        FastEvent? evt;
        try
        {
            evt = JsonSerializer.Deserialize<FastEvent>(result.Message.Value, Json.Options);
        }
        catch (JsonException ex)
        {
            evt = null;
            log.LogWarning("decode error topic={Topic} offset={Offset}: {Error}", result.Topic, result.Offset.Value, ex.Message);
        }
        if (evt is null)
        {
            Errors.WithLabels("decode").Inc();
            return;
        }

        var name = string.IsNullOrEmpty(evt.Event) ? result.Topic : evt.Event;
        var amount = (double)evt.Payload.Amount;

        // Business processing for the FAST transfer would go here.
        Consumed.WithLabels(name).Inc();
        if (result.Message.Timestamp.Type != TimestampType.NotAvailable)
        {
            Latency.WithLabels(name).Observe((DateTime.UtcNow - result.Message.Timestamp.UtcDateTime).TotalSeconds);
        }

        var partition = result.Partition.Value.ToString();
        ByPartition.WithLabels(settings.Group, result.Topic, partition).Inc();
        LastOffset.WithLabels(settings.Group, result.Topic, partition).Set(result.Offset.Value);
        log.LogInformation("consumed event={Event} transaction_id={TransactionId} amount={Amount:F2} TRY partition={Partition} offset={Offset}",
            name, evt.TransactionId, amount, result.Partition.Value, result.Offset.Value);
    }
}
