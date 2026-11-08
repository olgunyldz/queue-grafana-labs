package fast.reconciler;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Settings from environment variables (same style as the .NET services). */
record Config(
        String brokers,
        String applicationId,
        List<String> inputTopics,
        Set<String> sendEvents,
        Set<String> failedEvents,
        String matchedTopic,
        String unmatchedTopic,
        int partitions,
        Duration pendingThreshold,
        Duration matchTtl,
        Duration scanInterval,
        int standbyReplicas,
        String processingGuarantee,
        String stateDir,
        int metricsPort) {

    static Config fromEnv() {
        return new Config(
                env("KAFKA_BROKERS", "kafka:9092"),
                env("APPLICATION_ID", "fast-reconciler"),
                List.copyOf(csv(env("INPUT_TOPICS",
                        "fast_incoming_request,fast_incoming_success,fast_incoming_failed,"
                                + "fast_outgoing_request,fast_outgoing_success,fast_outgoing_failed"))),
                csv(env("SEND_EVENTS", "fast_incoming_request,fast_outgoing_request")),
                csv(env("FAILED_EVENTS", "fast_incoming_failed,fast_outgoing_failed")),
                env("MATCHED_TOPIC", "fast_transfer_matched"),
                env("UNMATCHED_TOPIC", "fast_transfer_unmatched"),
                Integer.parseInt(env("REPARTITION_PARTITIONS", "3")),
                duration("FAST_PENDING_THRESHOLD", Duration.ofSeconds(30)),
                duration("FAST_MATCH_TTL", Duration.ofMinutes(10)),
                duration("FAST_SCAN_INTERVAL", Duration.ofSeconds(5)),
                Integer.parseInt(env("NUM_STANDBY_REPLICAS", "1")),
                env("PROCESSING_GUARANTEE", "exactly_once_v2"),
                env("STATE_DIR", "/var/lib/fast-reconciler"),
                Integer.parseInt(env("METRICS_PORT", "9400")));
    }

    /** Comma-separated values, trimmed, empty entries dropped (insertion order kept). */
    static Set<String> csv(String raw) {
        return Arrays.stream(raw.split(","))
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .collect(Collectors.toCollection(java.util.LinkedHashSet::new));
    }

    static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value.trim();
    }

    /** Durations like "30s", "10m", "500ms", "1h". */
    static Duration duration(String key, Duration fallback) {
        String raw = env(key, "");
        if (raw.isEmpty()) {
            return fallback;
        }
        try {
            if (raw.endsWith("ms")) return Duration.ofMillis(Long.parseLong(raw.substring(0, raw.length() - 2)));
            if (raw.endsWith("s")) return Duration.ofSeconds(Long.parseLong(raw.substring(0, raw.length() - 1)));
            if (raw.endsWith("m")) return Duration.ofMinutes(Long.parseLong(raw.substring(0, raw.length() - 1)));
            if (raw.endsWith("h")) return Duration.ofHours(Long.parseLong(raw.substring(0, raw.length() - 1)));
        } catch (NumberFormatException ignored) {
            // fall through
        }
        System.err.printf("invalid %s=%s, using %s%n", key, raw, fallback);
        return fallback;
    }
}
