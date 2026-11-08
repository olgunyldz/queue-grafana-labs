package fast.reconciler.flink;

import java.util.Locale;

/**
 * Transfer flows reconciled by this job. Each flow runs as its own Flink job (in its own cluster),
 * selected with the FLOW environment variable, so FAST, Havale and EFT never share state, offsets,
 * Kafka transactions or metrics.
 *
 * <p>The enum holds the per-flow defaults; every value can still be overridden with an environment
 * variable. Flow-specific business rules belong here as well when the flows start to differ.
 */
enum Flow {
    FAST(
            "fast_incoming_request,fast_outgoing_request",
            "fast_incoming_success,fast_outgoing_success",
            "fast_incoming_failed,fast_outgoing_failed"),
    HAVALE(
            "havale_request",
            "havale_success",
            "havale_failed"),
    EFT(
            "eft_incoming_request,eft_outgoing_request",
            "eft_incoming_success,eft_outgoing_success",
            "eft_incoming_failed,eft_outgoing_failed");

    final String sendTopics;
    final String successTopics;
    final String failedTopics;

    Flow(String sendTopics, String successTopics, String failedTopics) {
        this.sendTopics = sendTopics;
        this.successTopics = successTopics;
        this.failedTopics = failedTopics;
    }

    /** Lower-case name used in ids, topics and the Prometheus "flow" label: fast, havale, eft. */
    String id() {
        return name().toLowerCase(Locale.ROOT);
    }

    String applicationId() {
        return "flink-" + id() + "-reconciler";
    }

    String matchedTopic() {
        return id() + "_transfer_matched";
    }

    String unmatchedTopic() {
        return id() + "_transfer_unmatched";
    }

    static Flow parse(String value) {
        try {
            return valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("FLOW must be one of fast, havale, eft; got: " + value, e);
        }
    }
}
