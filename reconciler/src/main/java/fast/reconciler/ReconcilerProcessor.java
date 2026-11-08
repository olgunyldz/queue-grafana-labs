package fast.reconciler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Matches a request event (SEND_EVENTS, e.g. fast_incoming_request) with its outcome (success / failed)
 * by transaction_id.
 *
 * <p>Input records are keyed by transaction_id (the topology repartitions by it), so all events of a
 * transfer reach the same task. Whichever side arrives first is kept in the state store; when the other
 * side arrives the transfer is emitted to the matched topic with the completion time computed from the
 * two Kafka record timestamps (event time, independent of read order).
 *
 * <p>The store is persistent (RocksDB) and backed by a changelog topic, so it survives restarts and moves
 * with its partition when instances are added or removed. A wall-clock punctuator updates the
 * waiting/stuck gauges and emits entries older than the TTL to the unmatched topic.
 *
 * <p>TTL and stuck ages are measured from "seen", the wall-clock time the entry entered the store, not from
 * the record timestamp: while a backlog is consumed an old request would otherwise expire on the next scan,
 * before its outcome (read from another topic moments later) arrives. After a match the key keeps a small
 * {"done"} marker for FAST_MATCH_TTL so a late duplicate is counted instead of becoming a new pending entry.
 *
 * <p>Output records carry a "fast-result" header ("matched" / "unmatched") used to route them to topics.
 */
final class ReconcilerProcessor implements Processor<String, String, String, String> {
    static final String STORE = "fast-transfer-state";
    static final String RESULT_HEADER = "fast-result";

    private static final Logger LOG = LoggerFactory.getLogger(ReconcilerProcessor.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    private final Config config;
    private final ReconcilerMetrics metrics;
    private ProcessorContext<String, String> context;
    private KeyValueStore<String, String> store;
    private String taskId;

    ReconcilerProcessor(Config config, ReconcilerMetrics metrics) {
        this.config = config;
        this.metrics = metrics;
    }

    @Override
    public void init(ProcessorContext<String, String> context) {
        this.context = context;
        this.store = context.getStateStore(STORE);
        this.taskId = context.taskId().toString();
        context.schedule(config.scanInterval(), PunctuationType.WALL_CLOCK_TIME, this::scan);
    }

    @Override
    public void process(Record<String, String> record) {
        JsonNode event;
        try {
            event = JSON.readTree(record.value());
        } catch (Exception e) {
            LOG.warn("skipping undecodable record key={}: {}", record.key(), e.getMessage());
            return;
        }
        String tx = text(event, "transaction_id");
        String name = text(event, "event");
        if (tx == null || name == null) {
            return;
        }
        JsonNode payload = event.path("payload");
        long ts = record.timestamp();
        long now = context.currentSystemTimeMs();
        String existing = store.get(tx);
        ObjectNode state = existing == null ? JSON.createObjectNode() : (ObjectNode) readTree(existing);

        if (state.has("done")) {
            metrics.duplicate(name);
            return;
        }

        if (config.sendEvents().contains(name)) {
            if (state.has("outcome")) {
                emitMatched(tx, sendSide(name, payload, ts), state.get("outcome"));
                markDone(tx, now);
            } else if (state.has("send")) {
                metrics.duplicate(name);
            } else {
                state.set("send", sendSide(name, payload, ts));
                state.put("seen", now);
                store.put(tx, state.toString());
            }
        } else {
            ObjectNode outcome = JSON.createObjectNode()
                    .put("event", name)
                    .put("ts", ts)
                    .put("amount", payload.path("amount").asDouble())
                    .put("failure_reason", text(payload, "failure_reason"))
                    .put("sender_bank", Banks.code(text(payload, "sender_bank"), text(payload, "sender_iban")))
                    .put("receiver_bank", Banks.code(text(payload, "receiver_bank"), text(payload, "receiver_iban")));
            if (state.has("send")) {
                emitMatched(tx, state.get("send"), outcome);
                markDone(tx, now);
            } else if (state.has("outcome")) {
                metrics.duplicate(name);
            } else {
                state.set("outcome", outcome);
                state.put("seen", now);
                store.put(tx, state.toString());
            }
        }
    }

    private void markDone(String tx, long now) {
        store.put(tx, JSON.createObjectNode().put("done", true).put("seen", now).toString());
    }

    private static ObjectNode sendSide(String name, JsonNode payload, long ts) {
        return JSON.createObjectNode()
                .put("event", name)
                .put("ts", ts)
                .put("amount", payload.path("amount").asDouble())
                .put("sender_bank", Banks.code(text(payload, "sender_bank"), text(payload, "sender_iban")))
                .put("receiver_bank", Banks.code(text(payload, "receiver_bank"), text(payload, "receiver_iban")));
    }

    /** Bank and amount come from fast_send (the source of truth); the outcome only adds result and time. */
    private void emitMatched(String tx, JsonNode send, JsonNode outcome) {
        long sendTs = send.path("ts").asLong();
        long outcomeTs = outcome.path("ts").asLong();
        String result = outcome.path("event").asText();
        double amount = send.path("amount").asDouble(outcome.path("amount").asDouble());
        String senderBank = firstNonNull(text(send, "sender_bank"), text(outcome, "sender_bank"));
        String receiverBank = firstNonNull(text(send, "receiver_bank"), text(outcome, "receiver_bank"));
        String reason = text(outcome, "failure_reason");
        Duration completion = Duration.ofMillis(Math.max(0, outcomeTs - sendTs));

        metrics.matched(result, completion, amount, senderBank, receiverBank, reason);

        String[] sender = Banks.labels(senderBank);
        String[] receiver = Banks.labels(receiverBank);
        ObjectNode out = JSON.createObjectNode()
                .put("transaction_id", tx)
                .put("outcome", result)
                .put("completion_ms", completion.toMillis())
                .put("amount", amount)
                .put("currency", "TRY")
                .put("sender_bank", senderBank)
                .put("sender_bank_name", sender[1])
                .put("receiver_bank", receiverBank)
                .put("receiver_bank_name", receiver[1])
                .put("failure_reason", reason)
                .put("sent_at", Instant.ofEpochMilli(sendTs).toString())
                .put("completed_at", Instant.ofEpochMilli(outcomeTs).toString());
        forward(tx, out, Math.max(sendTs, outcomeTs), "matched");
    }

    /** Wall-clock scan: gauges for this task and TTL expiry of unmatched entries. */
    private void scan(long now) {
        long pending = 0;
        long stuck = 0;
        long orphans = 0;
        long thresholdMs = config.pendingThreshold().toMillis();
        long ttlMs = config.matchTtl().toMillis();
        List<KeyValue<String, JsonNode>> expired = new ArrayList<>();
        List<String> doneExpired = new ArrayList<>();

        try (KeyValueIterator<String, String> it = store.all()) {
            while (it.hasNext()) {
                KeyValue<String, String> entry = it.next();
                JsonNode state = readTree(entry.value);
                if (state.has("done")) {
                    if (now - state.path("seen").asLong() > ttlMs) {
                        doneExpired.add(entry.key);
                    }
                    continue;
                }
                boolean isSend = state.has("send");
                JsonNode side = isSend ? state.get("send") : state.get("outcome");
                // Entries written before "seen" existed fall back to the record timestamp.
                long seen = state.has("seen") ? state.get("seen").asLong() : side.path("ts").asLong();
                long age = now - seen;
                if (age > ttlMs) {
                    expired.add(KeyValue.pair(entry.key, state));
                } else if (isSend) {
                    pending++;
                    if (age > thresholdMs) {
                        stuck++;
                    }
                } else {
                    orphans++;
                }
            }
        }

        for (KeyValue<String, JsonNode> entry : expired) {
            JsonNode state = entry.value;
            boolean isSend = state.has("send");
            JsonNode side = isSend ? state.get("send") : state.get("outcome");
            String kind = isSend ? "send_without_outcome" : "outcome_without_send";
            ObjectNode out = JSON.createObjectNode()
                    .put("transaction_id", entry.key)
                    .put("kind", kind)
                    .put("event", side.path("event").asText(isSend ? "request" : ""))
                    .put("amount", side.path("amount").asDouble())
                    .put("sender_bank", text(side, "sender_bank"))
                    .put("receiver_bank", text(side, "receiver_bank"))
                    .put("first_seen_at", Instant.ofEpochMilli(side.path("ts").asLong()).toString())
                    .put("expired_at", Instant.ofEpochMilli(now).toString());
            forward(entry.key, out, now, "unmatched");
            store.delete(entry.key);
            metrics.unmatched(kind).increment();
        }
        doneExpired.forEach(store::delete);
        metrics.taskState(taskId, pending, stuck, orphans);
    }

    private void forward(String key, ObjectNode value, long timestamp, String result) {
        RecordHeaders headers = new RecordHeaders();
        headers.add(RESULT_HEADER, result.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        context.forward(new Record<>(key, value.toString(), timestamp, headers));
    }

    @Override
    public void close() {
        metrics.taskClosed(taskId);
    }

    private static JsonNode readTree(String json) {
        try {
            return JSON.readTree(json);
        } catch (Exception e) {
            throw new IllegalStateException("corrupt state entry", e);
        }
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() || value.asText().isBlank() ? null : value.asText();
    }

    private static String firstNonNull(String a, String b) {
        return a != null ? a : b;
    }
}
