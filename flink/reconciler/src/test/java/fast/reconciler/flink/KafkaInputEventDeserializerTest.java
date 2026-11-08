package fast.reconciler.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class KafkaInputEventDeserializerTest {
    private static InputEvent parse(String json) {
        return KafkaInputEventDeserializer.parse(json.getBytes(StandardCharsets.UTF_8), 42);
    }

    @Test
    void parsesTheFieldsReconciliationNeeds() {
        InputEvent e = parse("{\"event\":\"eft_outgoing_failed\",\"transaction_id\":\"tx-1\",\"payload\":"
                + "{\"amount\":12.5,\"failure_reason\":\"TIMEOUT\",\"sender_bank\":\"00010\","
                + "\"receiver_iban\":\"TR000006200000000000000000\",\"description\":\"ignored\"}}");
        assertEquals("tx-1", e.transactionId);
        assertEquals("eft_outgoing_failed", e.event);
        assertEquals(12.5, e.amount);
        assertEquals("TIMEOUT", e.failureReason);
        assertEquals("00010", e.senderBank);
        assertNull(e.receiverBank);
        assertEquals("TR000006200000000000000000", e.receiverIban);
        assertEquals(42, e.timestamp);
    }

    @Test
    void rejectsInvalidRecords() {
        assertNull(parse("not json"));
        assertNull(parse("[1, 2]"));
        assertNull(parse("{\"event\":\"fast_incoming_request\"}"));
        assertNull(parse("{\"transaction_id\":\"tx-1\"}"));
        assertNull(parse("{\"event\":\"fast_incoming_request\",\"transaction_id\":\"  \"}"));
    }

    @Test
    void missingPayloadGivesDefaults() {
        InputEvent e = parse("{\"event\":\"havale_success\",\"transaction_id\":\"tx-2\",\"payload\":null}");
        assertEquals("tx-2", e.transactionId);
        assertEquals(0.0, e.amount);
        assertNull(e.senderBank);
    }
}
