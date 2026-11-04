using Prometheus;

namespace FastTransfer.Consumer;

/// <summary>
/// Pairs each fast_send with its outcome (fast_send_success / fast_send_failed) by transaction_id.
/// C# port of cmd/consumer/matcher.go.
///
/// The three topics are consumed in parallel, so an outcome can be read before its send. Both sides are
/// buffered and the completion time is computed from the Kafka write timestamps, not from read order.
/// State is in memory only; after a restart half-matched transfers are reported as expired after the TTL.
/// </summary>
public sealed class Matcher
{
    static readonly Histogram CompletionTime = Metrics.CreateHistogram("fast_transfer_completion_seconds",
        "Time from fast_send to its outcome (Kafka write timestamps), by outcome.",
        new HistogramConfiguration { LabelNames = ["outcome"], Buckets = [.05, .1, .25, .5, 1, 2, 5, 10, 30, 60, 120, 300, 600] });
    static readonly Gauge Pending = Metrics.CreateGauge("fast_transfers_pending", "fast_send events waiting for an outcome.");
    static readonly Gauge Stuck = Metrics.CreateGauge("fast_transfers_pending_over_threshold",
        "fast_send events waiting for an outcome longer than the threshold (FAST_PENDING_THRESHOLD, default 30s).");
    static readonly Gauge OutcomesWithoutSend = Metrics.CreateGauge("fast_outcomes_without_send",
        "Outcome events whose fast_send has not been seen (yet).");
    static readonly Counter Expired = Metrics.CreateCounter("fast_transfers_expired_total",
        "Unmatched entries dropped after the TTL (FAST_MATCH_TTL, default 10m), by kind.", "kind");
    static readonly Counter Duplicates = Metrics.CreateCounter("fast_duplicate_events_total",
        "Events whose transaction_id was already waiting on the same side, by event.", "event");

    readonly object _lock = new();
    readonly Dictionary<string, DateTime> _sends = new();
    readonly Dictionary<string, (string Event, DateTime At)> _outcomes = new();
    readonly TimeSpan _threshold;
    readonly TimeSpan _ttl;

    public Matcher(TimeSpan threshold, TimeSpan ttl)
    {
        _threshold = threshold;
        _ttl = ttl;
        CompletionTime.WithLabels("fast_send_success");
        CompletionTime.WithLabels("fast_send_failed");
        Expired.WithLabels("send_without_outcome");
        Expired.WithLabels("outcome_without_send");
    }

    public void Send(string tx, DateTime at)
    {
        lock (_lock)
        {
            if (_outcomes.Remove(tx, out var outcome))
            {
                Observe(outcome.Event, outcome.At - at);
                return;
            }
            if (!_sends.TryAdd(tx, at)) Duplicates.WithLabels("fast_send").Inc();
        }
    }

    public void Outcome(string tx, string evt, DateTime at)
    {
        lock (_lock)
        {
            if (_sends.Remove(tx, out var sentAt))
            {
                Observe(evt, at - sentAt);
                return;
            }
            if (!_outcomes.TryAdd(tx, (evt, at))) Duplicates.WithLabels(evt).Inc();
        }
    }

    static void Observe(string outcome, TimeSpan d) =>
        CompletionTime.WithLabels(outcome).Observe(Math.Max(0, d.TotalSeconds));

    /// <summary>Updates the gauges and drops entries older than the TTL.</summary>
    public void Sweep(DateTime now)
    {
        lock (_lock)
        {
            var stuck = 0;
            foreach (var (tx, at) in _sends.ToArray())
            {
                var age = now - at;
                if (age > _ttl)
                {
                    _sends.Remove(tx);
                    Expired.WithLabels("send_without_outcome").Inc();
                }
                else if (age > _threshold)
                {
                    stuck++;
                }
            }
            foreach (var (tx, o) in _outcomes.ToArray())
            {
                if (now - o.At > _ttl)
                {
                    _outcomes.Remove(tx);
                    Expired.WithLabels("outcome_without_send").Inc();
                }
            }
            Pending.Set(_sends.Count);
            Stuck.Set(stuck);
            OutcomesWithoutSend.Set(_outcomes.Count);
        }
    }
}
