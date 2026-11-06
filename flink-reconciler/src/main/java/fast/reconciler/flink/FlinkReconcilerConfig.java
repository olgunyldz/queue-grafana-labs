package fast.reconciler.flink;

import java.io.Serializable;

/** Environment-backed configuration for the Flink reconciler. */
record FlinkReconcilerConfig(
        String kafkaBrokers,
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

    private static final long serialVersionUID = 1L;

    static FlinkReconcilerConfig fromEnvironment() {
        return new FlinkReconcilerConfig(
                env("KAFKA_BROKERS", "kafka:9092"),
                env("APPLICATION_ID", "fast-flink-reconciler"),
                env("SEND_TOPIC", "fast_send"),
                env("SUCCESS_TOPIC", "fast_send_success"),
                env("FAILED_TOPIC", "fast_send_failed"),
                env("MATCHED_TOPIC", "fast_transfer_matched"),
                env("UNMATCHED_TOPIC", "fast_transfer_unmatched"),
                durationMs("FAST_PENDING_THRESHOLD", "FAST_PENDING_THRESHOLD_MS", 30_000),
                durationMs("FAST_MATCH_TTL", "FAST_MATCH_TTL_MS", 600_000),
                durationMs("CHECKPOINT_INTERVAL", "CHECKPOINT_INTERVAL_MS", 10_000),
                Integer.parseInt(env("PARALLELISM", "3")));
    }

    private static long durationMs(String durationKey, String legacyMsKey, long fallback) {
        String value = System.getenv(durationKey);
        if (value == null || value.isBlank()) {
            value = System.getenv(legacyMsKey);
        }
        if (value == null || value.isBlank()) {
            return fallback;
        }
        String normalized = value.trim().toLowerCase();
        if (normalized.endsWith("ms")) {
            return Long.parseLong(normalized.substring(0, normalized.length() - 2));
        }
        if (normalized.endsWith("s")) {
            return Long.parseLong(normalized.substring(0, normalized.length() - 1)) * 1_000;
        }
        if (normalized.endsWith("m")) {
            return Long.parseLong(normalized.substring(0, normalized.length() - 1)) * 60_000;
        }
        return Long.parseLong(normalized);
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value.trim();
    }
}
