using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Shared;
using Prometheus;

namespace MockProducer;

/// <summary>
/// HTTP API that writes transfer events to Kafka.
/// FAST, Havale and EFT success/failed endpoints are supported.
/// </summary>
public static class ProducerApp
{
    static readonly Counter Produced = Metrics.CreateCounter("producer_events_total",
        "Transfer events successfully delivered to Kafka, by HTTP endpoint.", "endpoint");
    static readonly Counter Errors = Metrics.CreateCounter("producer_errors_total",
        "Producer errors, by HTTP endpoint and reason.", "endpoint", "reason");
    static readonly Counter Amount = Metrics.CreateCounter("producer_amount_try_total",
        "Total TRY amount written to Kafka, by HTTP endpoint.", "endpoint");
    static readonly Counter HttpRequests = Metrics.CreateCounter("producer_http_requests_total",
        "HTTP requests to the FAST endpoints, by endpoint and status code.", "endpoint", "code");
    static readonly Histogram HttpDuration = Metrics.CreateHistogram("producer_http_request_duration_seconds",
        "HTTP request duration of the FAST endpoints (includes enqueueing to the Kafka client), by endpoint.",
        new HistogramConfiguration
        {
            LabelNames = ["endpoint"],
            Buckets = [.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10],
        });

    static long _sequence;

    public static WebApplication Build(string[] args)
    {
        var builder = WebApplication.CreateSlimBuilder(args);
        builder.WebHost.UseUrls(Env.Get("HTTP_ADDR", "http://0.0.0.0:8080"));

        var broker = Env.Get("KAFKA_BROKERS", "kafka:9092");
        builder.Services.AddSingleton<IProducer<string, string>>(_ =>
            new ProducerBuilder<string, string>(new ProducerConfig
            {
                BootstrapServers = broker,
                Acks = Acks.Leader,          // wait for the partition leader only
                LingerMs = 10,               // same as BatchTimeout: 10ms
                BatchNumMessages = 1000,
                QueueBufferingMaxMessages = 100_000,
                QueueBufferingMaxKbytes = 1_048_576,
                CompressionType = CompressionType.Lz4,
                Partitioner = Partitioner.Murmur2Random,
                ClientId = "fast-producer-dotnet",
            }).Build());

        var app = builder.Build();
        var events = Env.Get("EVENT_NAMES", string.Join(",", TransferEvents.All))
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        foreach (var e in events)
        {
            var endpoint = TransferEvents.EndpointPath(e);
            Produced.WithLabels(endpoint);
            Amount.WithLabels(endpoint);
        }

        app.MapGet("/healthz", () => Results.Text("ok"));
        app.MapMetrics("/metrics");

        app.Use(async (ctx, next) =>
        {
            if (!ctx.Request.Path.StartsWithSegments("/events", out var rest))
            {
                await next();
                return;
            }
            var eventName = EventName(ctx.Request.Path);
            var endpoint = events.Contains(eventName, StringComparer.Ordinal)
                ? TransferEvents.EndpointPath(eventName)
                : "unknown";
            var sw = Stopwatch.StartNew();
            await next();
            HttpDuration.WithLabels(endpoint).Observe(sw.Elapsed.TotalSeconds);
            HttpRequests.WithLabels(endpoint, ctx.Response.StatusCode.ToString()).Inc();
        });

        app.MapMethods("/events/{**path}", ["GET", "PUT", "PATCH", "DELETE"], (string path, HttpContext ctx) =>
        {
            var name = EventName(path);
            if (!events.Contains(name, StringComparer.Ordinal)) return UnknownEvent();
            ctx.Response.Headers.Allow = "POST";
            return Results.Text("method not allowed", statusCode: StatusCodes.Status405MethodNotAllowed);
        });

        app.MapPost("/events/{**path}", async (string path, HttpRequest request, IProducer<string, string> producer, ILoggerFactory lf) =>
        {
            var name = EventName(path);
            if (!events.Contains(name, StringComparer.Ordinal)) return UnknownEvent();

            Transfer? transfer;
            try
            {
                transfer = await JsonSerializer.DeserializeAsync<Transfer>(request.Body, Json.Options);
            }
            catch (JsonException)
            {
                transfer = null;
            }
            if (transfer is null)
            {
                Errors.WithLabels(TransferEvents.EndpointPath(name), "invalid_json").Inc();
                return Results.Text("body must be a valid transfer JSON object", statusCode: 400);
            }
            if (Validate(name, transfer) is { } error)
            {
                Errors.WithLabels(TransferEvents.EndpointPath(name), "validation").Inc();
                return Results.Text(error, statusCode: 400);
            }

            var evt = new TransferEvent
            {
                Id = Interlocked.Increment(ref _sequence).ToString(),
                Event = name,
                TransactionId = transfer.TransactionId,
                Payload = transfer,
                CreatedAt = DateTime.UtcNow.ToString("O"),
            };
            var endpoint = TransferEvents.EndpointPath(name);
            try
            {
                // Do not hold the HTTP request open while Kafka waits for an acknowledgement.
                // Confluent.Kafka queues the message in its bounded producer buffer and invokes
                // the callback when the broker accepts or rejects it.
                producer.Produce(name, new Message<string, string>
                {
                    Key = transfer.TransactionId,
                    Value = JsonSerializer.Serialize(evt, Json.Options),
                    Headers = new Headers
                    {
                        { "event", Encoding.UTF8.GetBytes(name) },
                        { "producer", "dotnet"u8.ToArray() },
                    },
                }, report =>
                {
                    if (report.Error.IsError)
                    {
                        Errors.WithLabels(endpoint, "kafka").Inc();
                        lf.CreateLogger("producer").LogError("publish {Event} failed: {Error}", name, report.Error.Reason);
                        return;
                    }

                    Produced.WithLabels(endpoint).Inc();
                    Amount.WithLabels(endpoint).Inc((double)transfer.Amount);
                });
            }
            catch (Exception ex) when (ex is ProduceException<string, string> or KafkaException)
            {
                Errors.WithLabels(endpoint, "kafka_queue").Inc();
                lf.CreateLogger("producer").LogError("publish {Event} failed: {Error}", name, ex.Message);
                return Results.Text("could not publish event", statusCode: 503);
            }

            // 202 means accepted by the local producer buffer. Delivery failures are exposed
            // through producer_errors_total and the delivery callback.
            return Results.Json(evt, Json.Options, statusCode: 202);
        });

        app.MapFallback((HttpContext ctx) => ctx.Request.Path.StartsWithSegments("/events")
            ? UnknownEvent()
            : Results.NotFound());

        app.Lifetime.ApplicationStopping.Register(() =>
            app.Services.GetRequiredService<IProducer<string, string>>().Flush(TimeSpan.FromSeconds(5)));

        app.Logger.LogInformation("producer listening; kafka={Broker} events={Events}", broker, string.Join(",", events));
        return app;
    }

    static string EventName(PathString path) => EventName(path.Value ?? "");

    static string EventName(string path)
    {
        var normalized = path.Trim('/');
        if (TransferEvents.TryGetEventName(normalized, out var directional)) return directional;
        if (TransferEvents.TryGetEventName($"events/{normalized}", out directional)) return directional;
        return normalized.StartsWith("events/", StringComparison.OrdinalIgnoreCase)
            ? normalized["events/".Length..]
            : normalized;
    }

    static IResult UnknownEvent() =>
        Results.Text("unknown event; expected a configured transfer success or failed endpoint", statusCode: 404);

    static string? Validate(string name, Transfer t)
    {
        if (string.IsNullOrWhiteSpace(t.TransactionId)) return "transaction_id is required";
        if (t.Amount <= 0) return "amount must be greater than 0";
        if (string.IsNullOrEmpty(t.Currency)) t.Currency = "TRY";
        if (t.Currency != "TRY") return "transfer events must be in TRY";
        if (name.EndsWith("_failed", StringComparison.Ordinal) && string.IsNullOrWhiteSpace(t.FailureReason))
            return "failure_reason is required for failed events";
        return null;
    }
}
