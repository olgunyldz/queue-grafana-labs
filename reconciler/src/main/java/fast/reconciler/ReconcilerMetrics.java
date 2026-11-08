package fast.reconciler;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Business metrics produced by the reconciler (Prometheus names in brackets). Same names as the Flink
 * reconciler; Prometheus adds impl="kstreams" and flow="fast" at scrape time:
 * <ul>
 *   <li>completion time per outcome and sender bank   [reconciler_completion_seconds]</li>
 *   <li>matched transfers per outcome / bank role / bank [reconciler_transfers_total]</li>
 *   <li>TRY volume per outcome / bank role / bank        [reconciler_volume_try_total]</li>
 *   <li>amount distribution of matched transfers         [reconciler_amount_try]</li>
 *   <li>failures per reason                              [reconciler_failures_total]</li>
 *   <li>unmatched entries dropped after the TTL          [reconciler_unmatched_total]</li>
 *   <li>duplicate events                                 [reconciler_duplicates_total]</li>
 *   <li>waiting / stuck / orphan entries (gauges)        [reconciler_pending, _stuck, _orphan_outcomes]</li>
 * </ul>
 * Counters are side effects: with exactly-once processing the output topics are exact, but a retried
 * batch can increment a counter twice. The topics (and their offsets) are the source of truth.
 */
final class ReconcilerMetrics {
    private static final Duration[] COMPLETION_BUCKETS = {
            Duration.ofMillis(50), Duration.ofMillis(100), Duration.ofMillis(250), Duration.ofMillis(500),
            Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(5), Duration.ofSeconds(10),
            Duration.ofSeconds(30), Duration.ofSeconds(60), Duration.ofSeconds(120), Duration.ofSeconds(300),
            Duration.ofSeconds(600)};
    private static final double[] AMOUNT_BUCKETS = {100, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000};

    private final MeterRegistry registry;
    private final java.util.Set<String> failedEvents;
    /** Per stream task: {pending, stuck, orphanOutcomes}. Gauges report the sum over this instance's tasks. */
    private final Map<String, long[]> taskState = new ConcurrentHashMap<>();

    ReconcilerMetrics(MeterRegistry registry, Config config) {
        this.registry = registry;
        this.failedEvents = config.failedEvents();
        Gauge.builder("reconciler.pending", taskState, s -> sum(s, 0))
                .description("Requests waiting for an outcome (this instance's tasks).").register(registry);
        Gauge.builder("reconciler.stuck", taskState, s -> sum(s, 1))
                .description("Requests waiting for an outcome longer than FAST_PENDING_THRESHOLD.").register(registry);
        Gauge.builder("reconciler.orphan.outcomes", taskState, s -> sum(s, 2))
                .description("Outcomes whose request has not been seen (yet).").register(registry);
        // Pre-register the outcome series so dashboards show 0 instead of "no data".
        for (String topic : config.inputTopics()) {
            if (!config.sendEvents().contains(topic)) {
                counterTransfers(topic, "sender", Banks.UNKNOWN, Banks.UNKNOWN);
            }
        }
        for (String kind : new String[] {"send_without_outcome", "outcome_without_send"}) {
            unmatched(kind);
        }
    }

    private static double sum(Map<String, long[]> state, int index) {
        return state.values().stream().mapToLong(v -> v[index]).sum();
    }

    void taskState(String taskId, long pending, long stuck, long orphans) {
        taskState.put(taskId, new long[] {pending, stuck, orphans});
    }

    void taskClosed(String taskId) {
        taskState.remove(taskId);
    }

    void matched(String outcome, Duration completion, double amount, String senderCode, String receiverCode, String reason) {
        String[] sender = Banks.labels(senderCode);
        String[] receiver = Banks.labels(receiverCode);
        Timer.builder("reconciler.completion")
                .description("Time from the request to its outcome (Kafka record timestamps).")
                .tags("outcome", outcome, "sender_bank", sender[1])
                .serviceLevelObjectives(COMPLETION_BUCKETS)
                .register(registry)
                .record(completion.isNegative() ? Duration.ZERO : completion);
        for (String[] role : new String[][] {{"sender", sender[0], sender[1]}, {"receiver", receiver[0], receiver[1]}}) {
            counterTransfers(outcome, role[0], role[1], role[2]).increment();
            Counter.builder("reconciler.volume").baseUnit("try")
                    .description("TRY volume of matched transfers by outcome, bank role and bank.")
                    .tags("outcome", outcome, "role", role[0], "bank_code", role[1], "bank", role[2])
                    .register(registry).increment(amount);
        }
        DistributionSummary.builder("reconciler.amount").baseUnit("try")
                .description("Amount distribution of matched transfers (TRY).")
                .tags("outcome", outcome)
                .serviceLevelObjectives(AMOUNT_BUCKETS)
                .register(registry).record(amount);
        if (failedEvents.contains(outcome)) {
            Counter.builder("reconciler.failures")
                    .description("Failed transfers by failure reason.")
                    .tags("reason", reason == null || reason.isBlank() ? "unknown" : reason)
                    .register(registry).increment();
        }
    }

    private Counter counterTransfers(String outcome, String role, String code, String bank) {
        return Counter.builder("reconciler.transfers")
                .description("Matched transfers by outcome, bank role (sender/receiver) and bank.")
                .tags("outcome", outcome, "role", role, "bank_code", code, "bank", bank)
                .register(registry);
    }

    Counter unmatched(String kind) {
        return Counter.builder("reconciler.unmatched")
                .description("Entries dropped after FAST_MATCH_TTL without a match, by kind.")
                .tags("kind", kind).register(registry);
    }

    void duplicate(String event) {
        Counter.builder("reconciler.duplicates")
                .description("Events whose transaction_id was already waiting on the same side or already matched.")
                .tags("event", event).register(registry).increment();
    }
}
