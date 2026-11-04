using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using FastTransfer.Shared;
using Prometheus;

namespace FastTransfer.Producer;

/// <summary>
/// HTTP API that writes FAST events to Kafka.
/// POST /events/{fast_send|fast_send_success|fast_send_failed}; each event goes to the topic of the same name.
/// </summary>
public static class ProducerApp
{
    static readonly Counter Produced = Metrics.CreateCounter("producer_events_total",
        "FAST events successfully written to Kafka, by event.", "event");
    static readonly Counter Errors = Metrics.CreateCounter("producer_errors_total",
        "Producer errors, by event and reason.", "event", "reason");
    static readonly Counter Amount = Metrics.CreateCounter("producer_fast_amount_try_total",
        "Total TRY amount of FAST events written to Kafka, by event.", "event");
    static readonly Counter HttpRequests = Metrics.CreateCounter("producer_http_requests_total",
        "HTTP requests to the FAST endpoints, by endpoint and status code.", "endpoint", "code");
    static readonly Histogram HttpDuration = Metrics.CreateHistogram("producer_http_request_duration_seconds",
        "HTTP request duration of the FAST endpoints (includes the Kafka write), by endpoint.",
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
                Partitioner = Partitioner.Murmur2Random,
                ClientId = "fast-producer-dotnet",
            }).Build());

        var app = builder.Build();
        foreach (var e in FastEvents.All)
        {
            Produced.WithLabels(e);
            Amount.WithLabels(e);
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
            var endpoint = rest.Value?.TrimStart('/') ?? "";
            if (!FastEvents.IsKnown(endpoint)) endpoint = "unknown";
            var sw = Stopwatch.StartNew();
            await next();
            HttpDuration.WithLabels(endpoint).Observe(sw.Elapsed.TotalSeconds);
            HttpRequests.WithLabels(endpoint, ctx.Response.StatusCode.ToString()).Inc();
        });

        app.MapMethods("/events/{name}", ["GET", "PUT", "PATCH", "DELETE"], (string name, HttpContext ctx) =>
        {
            if (!FastEvents.IsKnown(name)) return UnknownEvent();
            ctx.Response.Headers.Allow = "POST";
            return Results.Text("method not allowed", statusCode: StatusCodes.Status405MethodNotAllowed);
        });

        app.MapPost("/events/{name}", async (string name, HttpRequest request, IProducer<string, string> producer, ILoggerFactory lf) =>
        {
            if (!FastEvents.IsKnown(name)) return UnknownEvent();

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
                Errors.WithLabels(name, "invalid_json").Inc();
                return Results.Text("body must be a valid FAST transfer JSON object", statusCode: 400);
            }
            if (Validate(name, transfer) is { } error)
            {
                Errors.WithLabels(name, "validation").Inc();
                return Results.Text(error, statusCode: 400);
            }

            var evt = new FastEvent
            {
                Id = Interlocked.Increment(ref _sequence).ToString(),
                Event = name,
                TransactionId = transfer.TransactionId,
                Payload = transfer,
                CreatedAt = DateTime.UtcNow.ToString("O"),
            };
            try
            {
                await producer.ProduceAsync(name, new Message<string, string>
                {
                    Key = transfer.TransactionId,
                    Value = JsonSerializer.Serialize(evt, Json.Options),
                    Headers = new Headers
                    {
                        { "event", Encoding.UTF8.GetBytes(name) },
                        { "producer", "dotnet"u8.ToArray() },
                    },
                }, request.HttpContext.RequestAborted);
            }
            catch (Exception ex) when (ex is ProduceException<string, string> or KafkaException)
            {
                Errors.WithLabels(name, "kafka").Inc();
                lf.CreateLogger("producer").LogError("publish {Event} failed: {Error}", name, ex.Message);
                return Results.Text("could not publish event", statusCode: 503);
            }

            Produced.WithLabels(name).Inc();
            Amount.WithLabels(name).Inc((double)transfer.Amount);
            return Results.Json(evt, Json.Options, statusCode: 202);
        });

        app.MapFallback((HttpContext ctx) => ctx.Request.Path.StartsWithSegments("/events")
            ? UnknownEvent()
            : Results.NotFound());

        app.Lifetime.ApplicationStopping.Register(() =>
            app.Services.GetRequiredService<IProducer<string, string>>().Flush(TimeSpan.FromSeconds(5)));

        app.Logger.LogInformation("producer listening; kafka={Broker} events={Events}", broker, string.Join(",", FastEvents.All));
        return app;
    }

    static IResult UnknownEvent() =>
        Results.Text("unknown event; expected one of fast_send, fast_send_success, fast_send_failed", statusCode: 404);

    static string? Validate(string name, Transfer t)
    {
        if (string.IsNullOrWhiteSpace(t.TransactionId)) return "transaction_id is required";
        if (t.Amount <= 0) return "amount must be greater than 0";
        if (string.IsNullOrEmpty(t.Currency)) t.Currency = "TRY";
        if (t.Currency != "TRY") return "FAST transfers must be in TRY";
        if (name == FastEvents.Failed && string.IsNullOrWhiteSpace(t.FailureReason))
            return "failure_reason is required for fast_send_failed";
        return null;
    }
}
