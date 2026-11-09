package fast.reconciler.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SagaFunctionTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final long MIN = 60_000;
    private KeyedOneInputStreamOperatorTestHarness<String, InputEvent, OutputRecord> harness;

    @BeforeEach
    void setUp() throws Exception {
        FlinkReconcilerConfig config = new FlinkReconcilerConfig(
                "dummy:9092", "fast", "flink-fast-reconciler-test",
                "fast_incoming_request", "fast_incoming_success", "fast_incoming_failed",
                "fast_transfer_matched", "fast_transfer_unmatched", 30_000, 600_000, 10_000, 1);
        // 30 s per command, 3 sends, 10 min for the FAST outcome.
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new SagaFunction(config, SagaSettings.defaults())),
                InputEvent::key,
                BasicTypeInfo.STRING_TYPE_INFO,
                1,
                1,
                0);
        harness.open();
        harness.setProcessingTime(0);
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    @Test
    void happyPathBlocksSettlesAndCompletes() throws Exception {
        request("tx-1", 0);
        List<OutputRecord> out = drain();
        assertEquals(List.of("ledger_commands", "fast_transfer_status"), topics(out));
        assertEquals("BLOCK", json(out.get(0)).get("command").asText());
        assertEquals("REQUESTED", json(out.get(1)).get("state").asText());

        ledger("BLOCKED", "tx-1", 100);
        assertEquals("BLOCKED", lastState());

        outcome("fast_incoming_success", "tx-1", 200);
        out = drain();
        assertEquals("SETTLE", json(out.get(0)).get("command").asText());
        assertEquals("SETTLING", json(out.get(1)).get("state").asText());

        ledger("SETTLED", "tx-1", 300);
        assertEquals("COMPLETED", lastState());
    }

    @Test
    void blockRejectedEndsWithoutSending() throws Exception {
        request("tx-2", 0);
        drain();
        ledger("BLOCK_REJECTED", "tx-2", 100);
        assertEquals("REJECTED", lastState());
        // A FAST outcome after a rejected block must not settle anything.
        outcome("fast_incoming_success", "tx-2", 200);
        assertTrue(drain().isEmpty());
    }

    @Test
    void failedOutcomeReleasesTheBlock() throws Exception {
        request("tx-3", 0);
        drain();
        ledger("BLOCKED", "tx-3", 100);
        drain();
        outcome("fast_incoming_failed", "tx-3", 200);
        List<OutputRecord> out = drain();
        assertEquals("RELEASE", json(out.get(0)).get("command").asText());
        ledger("RELEASED", "tx-3", 300);
        assertEquals("FAILED", lastState());
    }

    @Test
    void outcomeBeforeRequestIsKeptUntilTheBlockIsConfirmed() throws Exception {
        outcome("fast_incoming_failed", "tx-4", 0);
        assertTrue(drain().isEmpty());
        request("tx-4", 100);
        drain();
        ledger("BLOCKED", "tx-4", 200);
        List<OutputRecord> out = drain();
        assertEquals("RELEASE", json(command(out)).get("command").asText());
        assertEquals("RELEASING", json(out.get(out.size() - 1)).get("state").asText());
    }

    @Test
    void unansweredBlockIsSentAgainThenGoesToManualReview() throws Exception {
        request("tx-5", 0);
        drain();

        harness.setProcessingTime(30_000);
        List<OutputRecord> out = drain();
        assertEquals("BLOCK", json(out.get(0)).get("command").asText());
        assertEquals(2, json(out.get(0)).get("attempt").asInt());

        harness.setProcessingTime(60_000);
        assertEquals(3, json(drain().get(0)).get("attempt").asInt());

        harness.setProcessingTime(90_000);
        out = drain();
        assertEquals("MANUAL_REVIEW", json(out.get(0)).get("state").asText());
        assertTrue(json(out.get(0)).get("failure_reason").asText().contains("BLOCK"));
    }

    @Test
    void blockedWithoutFastOutcomeGoesToManualReview() throws Exception {
        request("tx-6", 0);
        ledger("BLOCKED", "tx-6", 1_000);
        drain();

        harness.setProcessingTime(10 * MIN);
        assertTrue(drain().isEmpty(), "no decision before the outcome timeout");
        harness.setProcessingTime(10 * MIN + 1_000);
        List<OutputRecord> out = drain();
        assertEquals("MANUAL_REVIEW", json(out.get(out.size() - 1)).get("state").asText());
    }

    @Test
    void duplicateRequestDoesNotBlockTwice() throws Exception {
        request("tx-7", 0);
        request("tx-7", 100);
        List<OutputRecord> out = drain();
        assertEquals(1, out.stream().filter(r -> r.topic.equals("ledger_commands")).count());
    }

    @Test
    void settledOrFinishedTransfersAreNotBlockedAgain() throws Exception {
        request("tx-8", 0);
        ledger("BLOCKED", "tx-8", 100);
        outcome("fast_incoming_success", "tx-8", 200);
        ledger("SETTLED", "tx-8", 300);
        drain();
        // Late duplicate of the request, long after completion but before the cleanup timer fires.
        request("tx-8", 400);
        assertTrue(drain().isEmpty());
    }

    @Test
    void measuresOpenBlocksAndHeldAmount() throws Exception {
        request("tx-9", 0);
        assertEquals(1, saga().countOpen(1), "REQUESTED");
        ledger("BLOCKED", "tx-9", 100);
        drain();
        assertEquals(1, saga().countOpen(2), "BLOCKED");
        assertEquals(12_550, saga().openAmountKurus(), "125.50 TRY held");
        outcome("fast_incoming_success", "tx-9", 200);
        drain();
        assertEquals(12_550, saga().openAmountKurus(), "still held while settling");
        ledger("SETTLED", "tx-9", 300);
        drain();
        assertEquals(0, saga().openAmountKurus(), "released from the open map once completed");
        assertEquals(0, saga().countOpen(2));
    }

    @Test
    void openSagasSurviveACheckpoint() throws Exception {
        request("tx-10", 0);
        ledger("BLOCKED", "tx-10", 100);
        drain();
        var snapshot = harness.snapshot(1, 1);
        harness.close();

        FlinkReconcilerConfig config = new FlinkReconcilerConfig(
                "dummy:9092", "fast", "flink-fast-reconciler-test",
                "fast_incoming_request", "fast_incoming_success", "fast_incoming_failed",
                "fast_transfer_matched", "fast_transfer_unmatched", 30_000, 600_000, 10_000, 1);
        SagaFunction restored = new SagaFunction(config, SagaSettings.defaults());
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(restored),
                InputEvent::key,
                BasicTypeInfo.STRING_TYPE_INFO,
                1,
                1,
                0);
        harness.setup();
        harness.initializeState(snapshot);
        harness.open();
        assertEquals(12_550, restored.openAmountKurus(), "the held amount is restored");
        assertEquals(1, restored.countOpen(2));
    }

    // ---- helpers ---------------------------------------------------------------------------

    /** Outputs since the previous call: extractOutputValues() returns everything collected so far. */
    private List<OutputRecord> drain() {
        List<OutputRecord> all = harness.extractOutputValues();
        harness.getOutput().clear();
        return all;
    }

    private void request(String tx, long at) throws Exception {
        harness.setProcessingTime(at);
        harness.processElement(new StreamRecord<>(event("fast_incoming_request", tx, at, "")));
    }

    private void outcome(String name, String tx, long at) throws Exception {
        harness.setProcessingTime(at);
        String extra = name.endsWith("failed") ? ",\"failure_reason\":\"TIMEOUT\"" : "";
        harness.processElement(new StreamRecord<>(event(name, tx, at, extra)));
    }

    private void ledger(String result, String tx, long at) throws Exception {
        harness.setProcessingTime(at);
        String json = "{\"event\":\"" + result + "\",\"transaction_id\":\"" + tx + "\"}";
        harness.processElement(new StreamRecord<>(
                KafkaInputEventDeserializer.parse(json.getBytes(StandardCharsets.UTF_8), at)));
    }

    /** The saga function of the running harness: the gauges are read from it. */
    private SagaFunction saga() throws Exception {
        return (SagaFunction) ((KeyedProcessOperator<?, ?, ?>) harness.getOperator()).getUserFunction();
    }

    private String lastState() {
        List<OutputRecord> out = drain();
        assertTrue(!out.isEmpty(), "expected a status record");
        return json(out.get(out.size() - 1)).get("state").asText();
    }

    private static InputEvent event(String name, String tx, long ts, String extra) {
        String json = "{\"event\":\"" + name + "\",\"transaction_id\":\"" + tx
                + "\",\"payload\":{\"amount\":125.5,\"sender_bank\":\"00010\",\"receiver_bank\":\"00046\"" + extra + "}}";
        return KafkaInputEventDeserializer.parse(json.getBytes(StandardCharsets.UTF_8), ts);
    }

    private static JsonNode json(OutputRecord record) {
        try {
            return JSON.readTree(record.value);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** The first ledger command in the outputs. */
    private static OutputRecord command(List<OutputRecord> records) {
        return records.stream().filter(r -> r.topic.equals("ledger_commands")).findFirst().orElseThrow();
    }

    private static List<String> topics(List<OutputRecord> records) {
        return records.stream().map(r -> r.topic).toList();
    }
}
