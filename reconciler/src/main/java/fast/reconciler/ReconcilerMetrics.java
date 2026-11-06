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
 * FAST business metrics produced by the reconciler (Prometheus names in brackets):
 * <ul>
 *   <li>completion time per outcome and sender bank   [fast_kstreams_reconciler_completion_seconds]</li>
 *   <li>matched transfers per outcome / bank role / bank [fast_kstreams_reconciler_transfers_total]</li>
 *   <li>TRY volume per outcome / bank role / bank        [fast_kstreams_reconciler_volume_try_total]</li>
 *   <li>amount distribution of matched transfers         [fast_kstreams_reconciler_amount_try]</li>
 *   <li>failures per reason                              [fast_kstreams_reconciler_failures_total]</li>
 *   <li>unmatched entries dropped after the TTL          [fast_kstreams_reconciler_unmatched_total]</li>
 *   <li>duplicate events                                 [fast_kstreams_reconciler_duplicates_total]</li>
 *   <li>waiting / stuck / orphan entries (gauges)        [fast_kstreams_reconciler_pending, _stuck, _orphan_outcomes]</li>
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
    /** Per stream task: {pending, stuck, orphanOutcomes}. Gauges report the sum over this instance's tasks. */
    private final Map<String, long[]> taskState = new ConcurrentHashMap<>();

    ReconcilerMetrics(MeterRegistry registry) {
        this.registry = registry;
        Gauge.builder("fast.kstreams.reconciler.pending", taskState, s -> sum(s, 0))
                .description("fast_send waiting for an outcome (this instance's tasks).").register(registry);
        Gauge.builder("fast.kstreams.reconciler.stuck", taskState, s -> sum(s, 1))
                .description("fast_send waiting for an outcome longer than FAST_PENDING_THRESHOLD.").register(registry);
        Gauge.builder("fast.kstreams.reconciler.orphan.outcomes", taskState, s -> sum(s, 2))
                .description("Outcomes whose fast_send has not been seen (yet).").register(registry);
        for (String outcome : new String[] {"fast_send_success", "fast_send_failed"}) {
            counterTransfers(outcome, "sender", Banks.UNKNOWN, Banks.UNKNOWN);
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
        Timer.builder("fast.kstreams.reconciler.completion")
                .description("Time from fast_send to its outcome (Kafka record timestamps).")
                .tags("outcome", outcome, "sender_bank", sender[1])
                .serviceLevelObjectives(COMPLETION_BUCKETS)
                .register(registry)
                .record(completion.isNegative() ? Duration.ZERO : completion);
        for (String[] role : new String[][] {{"sender", sender[0], sender[1]}, {"receiver", receiver[0], receiver[1]}}) {
            counterTransfers(outcome, role[0], role[1], role[2]).increment();
            Counter.builder("fast.kstreams.reconciler.volume").baseUnit("try")
                    .description("TRY volume of matched transfers by outcome, bank role and bank.")
                    .tags("outcome", outcome, "role", role[0], "bank_code", role[1], "bank", role[2])
                    .register(registry).increment(amount);
        }
        DistributionSummary.builder("fast.kstreams.reconciler.amount").baseUnit("try")
                .description("Amount distribution of matched transfers (TRY).")
                .tags("outcome", outcome)
                .serviceLevelObjectives(AMOUNT_BUCKETS)
                .register(registry).record(amount);
        if ("fast_send_failed".equals(outcome)) {
            Counter.builder("fast.kstreams.reconciler.failures")
                    .description("Failed transfers by failure reason.")
                    .tags("reason", reason == null || reason.isBlank() ? "unknown" : reason)
                    .register(registry).increment();
        }
    }

    private Counter counterTransfers(String outcome, String role, String code, String bank) {
        return Counter.builder("fast.kstreams.reconciler.transfers")
                .description("Matched transfers by outcome, bank role (sender/receiver) and bank.")
                .tags("outcome", outcome, "role", role, "bank_code", code, "bank", bank)
                .register(registry);
    }

    Counter unmatched(String kind) {
        return Counter.builder("fast.kstreams.reconciler.unmatched")
                .description("Entries dropped after FAST_MATCH_TTL without a match, by kind.")
                .tags("kind", kind).register(registry);
    }

    void duplicate(String event) {
        Counter.builder("fast.kstreams.reconciler.duplicates")
                .description("Events whose transaction_id was already waiting on the same side.")
                .tags("event", event).register(registry).increment();
    }
}
