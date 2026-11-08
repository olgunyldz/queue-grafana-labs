package fast.reconciler.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Properties;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FlinkReconciliationFunctionTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private KeyedOneInputStreamOperatorTestHarness<String, InputEvent, OutputRecord> harness;

    @BeforeEach
    void setUp() throws Exception {
        FlinkReconcilerConfig config = new FlinkReconcilerConfig(
                "dummy:9092", "fast", "fast-flink-test",
                "fast_send,fast_outgoing_request", "fast_send_success", "fast_send_failed,fast_outgoing_failed",
                "fast_transfer_matched", "fast_transfer_unmatched", 30_000, 600_000, 10_000, 1);
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new FlinkReconciliationFunction(config)),
                FlinkReconciliationFunction::transactionId,
                BasicTypeInfo.STRING_TYPE_INFO,
                1,
                1,
                0);
        harness.open();
        // The test processing-time service starts at Long.MIN_VALUE; start the clock at 0.
        harness.setProcessingTime(0);
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    @Test
    void matchesSendAndSuccess() throws Exception {
        harness.processElement(new StreamRecord<>(event(
                "fast_send", "tx-1", 1250.75,
                ",\"sender_bank\":\"00010\",\"receiver_bank\":\"00046\"", 1_000)));
        harness.processElement(new StreamRecord<>(event(
                "fast_send_success", "tx-1", 1250.75, "", 1_800)));

        List<OutputRecord> outputs = harness.extractOutputValues();
        assertEquals(1, outputs.size());
        JsonNode result = JSON.readTree(outputs.get(0).value);
        assertEquals("fast_transfer_matched", outputs.get(0).topic);
        assertEquals("tx-1", result.get("transaction_id").asText());
        assertEquals("fast_send_success", result.get("outcome").asText());
        assertEquals(800, result.get("completion_ms").asLong());
        assertEquals("00010", result.get("sender_bank").asText());
    }

    @Test
    void matchesOutcomeThatArrivesBeforeSend() throws Exception {
        harness.processElement(new StreamRecord<>(event(
                "fast_send_failed", "tx-2", 99.9,
                ",\"failure_reason\":\"INSUFFICIENT_BALANCE\"", 2_000)));
        harness.processElement(new StreamRecord<>(event(
                "fast_send", "tx-2", 99.9, ",\"sender_bank\":\"00231\"", 0)));

        List<OutputRecord> outputs = harness.extractOutputValues();
        assertEquals(1, outputs.size());
        JsonNode result = JSON.readTree(outputs.get(0).value);
        assertEquals("fast_send_failed", result.get("outcome").asText());
        assertEquals(2_000, result.get("completion_ms").asLong());
        assertEquals("INSUFFICIENT_BALANCE", result.get("failure_reason").asText());
    }

    @Test
    void expiresPendingSendAfterTtl() throws Exception {
        harness.processElement(new StreamRecord<>(event("fast_send", "tx-3", 10, "", 0)));
        harness.setProcessingTime(30_000);
        assertTrue(harness.extractOutputValues().isEmpty());

        harness.setProcessingTime(600_000);
        List<OutputRecord> outputs = harness.extractOutputValues();
        assertEquals(1, outputs.size());
        JsonNode result = JSON.readTree(outputs.get(0).value);
        assertEquals("fast_transfer_unmatched", outputs.get(0).topic);
        assertEquals("send_without_outcome", result.get("kind").asText());
    }

    @Test
    void matchesSendFromAnyConfiguredSendTopic() throws Exception {
        harness.processElement(new StreamRecord<>(event("fast_outgoing_request", "tx-4", 50, "", 1_000)));
        harness.processElement(new StreamRecord<>(event(
                "fast_outgoing_failed", "tx-4", 50, ",\"failure_reason\":\"TIMEOUT\"", 1_500)));

        List<OutputRecord> outputs = harness.extractOutputValues();
        assertEquals(1, outputs.size());
        assertEquals("fast_transfer_matched", outputs.get(0).topic);
        assertEquals("fast_outgoing_failed", JSON.readTree(outputs.get(0).value).get("outcome").asText());
    }

    @Test
    void oldBacklogEventIsNotExpiredBeforeItsOutcomeArrives() throws Exception {
        // Record timestamps are an hour older than the reconciler clock (catching up on a backlog).
        harness.setProcessingTime(3_600_000);
        harness.processElement(new StreamRecord<>(event("fast_send", "tx-5", 10, "", 1)));
        harness.setProcessingTime(3_600_001);
        assertTrue(harness.extractOutputValues().isEmpty());

        harness.processElement(new StreamRecord<>(event("fast_send_success", "tx-5", 10, "", 501)));
        List<OutputRecord> outputs = harness.extractOutputValues();
        assertEquals(1, outputs.size());
        assertEquals("fast_transfer_matched", outputs.get(0).topic);
        assertEquals(500, JSON.readTree(outputs.get(0).value).get("completion_ms").asLong());
    }

    @Test
    void duplicateAfterMatchDoesNotBecomeUnmatched() throws Exception {
        harness.processElement(new StreamRecord<>(event("fast_send", "tx-6", 10, "", 1_000)));
        harness.processElement(new StreamRecord<>(event("fast_send_success", "tx-6", 10, "", 1_200)));
        harness.processElement(new StreamRecord<>(event("fast_send", "tx-6", 10, "", 1_300)));

        harness.setProcessingTime(2 * 600_000);
        List<OutputRecord> outputs = harness.extractOutputValues();
        assertEquals(1, outputs.size());
        assertEquals("fast_transfer_matched", outputs.get(0).topic);
        assertEquals(0, harness.numKeyedStateEntries());
    }

    @Test
    void restoresGaugesFromCheckpoint() throws Exception {
        harness.processElement(new StreamRecord<>(event("fast_send", "tx-7", 10, "", 1_000)));
        var snapshot = harness.snapshot(1, 1);
        harness.close();

        FlinkReconcilerConfig config = new FlinkReconcilerConfig(
                "dummy:9092", "fast", "fast-flink-test", "fast_send", "fast_send_success", "fast_send_failed",
                "fast_transfer_matched", "fast_transfer_unmatched", 30_000, 600_000, 10_000, 1);
        FlinkReconciliationFunction restored = new FlinkReconciliationFunction(config);
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(restored),
                FlinkReconciliationFunction::transactionId,
                BasicTypeInfo.STRING_TYPE_INFO, 1, 1, 0);
        harness.setup();
        harness.initializeState(snapshot);
        harness.open();
        assertEquals(1, restored.metrics().pending());

        // The restored pending send still matches and the gauge goes back to 0 (not -1).
        harness.processElement(new StreamRecord<>(event("fast_send_success", "tx-7", 10, "", 1_100)));
        assertEquals(1, harness.extractOutputValues().size());
        assertEquals(0, restored.metrics().pending());
    }

    private static InputEvent event(String name, String transactionId, double amount, String extra, long timestamp) {
        String value = "{\"event\":\"" + name + "\",\"transaction_id\":\"" + transactionId
                + "\",\"payload\":{\"transaction_id\":\"" + transactionId
                + "\",\"amount\":" + amount + extra + "}}";
        return new InputEvent(value, timestamp);
    }
}
