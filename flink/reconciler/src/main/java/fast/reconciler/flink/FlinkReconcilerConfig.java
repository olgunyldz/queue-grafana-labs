package fast.reconciler.flink;

import java.io.Serializable;
import java.util.Locale;

/**
 * Environment-backed configuration. FLOW (fast | havale | eft) selects the defaults; any other
 * variable overrides the flow default.
 */
record FlinkReconcilerConfig(
        String kafkaBrokers,
        String flow,
        String applicationId,
        String sendTopic,
        String successTopic,
        String failedTopic,
        String matchedTopic,
        String unmatchedTopic,
        long pendingThresholdMs,
        long matchTtlMs,
        long checkpointIntervalMs,
        int parallelism) implements Serializable {

    private static final long serialVersionUID = 2L;

    static FlinkReconcilerConfig fromEnvironment() {
        Flow flow = Flow.parse(env("FLOW", "fast"));
        return new FlinkReconcilerConfig(
                env("KAFKA_BROKERS", "kafka:9092"),
                flow.id(),
                env("APPLICATION_ID", flow.applicationId()),
                env("SEND_TOPIC", flow.sendTopics),
                env("SUCCESS_TOPIC", flow.successTopics),
                env("FAILED_TOPIC", flow.failedTopics),
                env("MATCHED_TOPIC", flow.matchedTopic()),
                env("UNMATCHED_TOPIC", flow.unmatchedTopic()),
                durationMs("PENDING_THRESHOLD", "FAST_PENDING_THRESHOLD_MS", 30_000),
                durationMs("MATCH_TTL", "FAST_MATCH_TTL_MS", 600_000),
                durationMs("CHECKPOINT_INTERVAL", "CHECKPOINT_INTERVAL_MS", 10_000),
                Integer.parseInt(env("PARALLELISM", "3")));
    }

    /** Durations like "500ms", "30s", "10m", "1h"; a bare number is milliseconds. */
    static long durationMs(String durationKey, String legacyMsKey, long fallback) {
        String value = System.getenv(durationKey);
        String key = durationKey;
        if (value == null || value.isBlank()) {
            value = System.getenv(legacyMsKey);
            key = legacyMsKey;
        }
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return parseDurationMs(key, value);
    }

    static long parseDurationMs(String key, String value) {
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        try {
            if (normalized.endsWith("ms")) {
                return Long.parseLong(normalized.substring(0, normalized.length() - 2));
            }
            if (normalized.endsWith("s")) {
                return Long.parseLong(normalized.substring(0, normalized.length() - 1)) * 1_000;
            }
            if (normalized.endsWith("m")) {
                return Long.parseLong(normalized.substring(0, normalized.length() - 1)) * 60_000;
            }
            if (normalized.endsWith("h")) {
                return Long.parseLong(normalized.substring(0, normalized.length() - 1)) * 3_600_000;
            }
            return Long.parseLong(normalized);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "invalid duration " + key + "=" + value + " (expected e.g. 500ms, 30s, 10m, 1h)", e);
        }
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value.trim();
    }
}
