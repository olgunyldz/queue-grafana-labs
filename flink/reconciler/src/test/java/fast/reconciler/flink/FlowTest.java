package fast.reconciler.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class FlowTest {
    @Test
    void eachFlowHasItsOwnTopicsAndIds() {
        assertEquals("havale_request", Flow.HAVALE.sendTopics);
        assertEquals("havale_transfer_matched", Flow.HAVALE.matchedTopic());
        assertEquals("flink-havale-reconciler", Flow.HAVALE.applicationId());
        assertEquals("eft_incoming_failed,eft_outgoing_failed", Flow.EFT.failedTopics);
        assertEquals("fast_transfer_unmatched", Flow.FAST.unmatchedTopic());
    }

    @Test
    void parsesFlowCaseInsensitivelyAndRejectsUnknown() {
        assertEquals(Flow.EFT, Flow.parse(" Eft "));
        assertThrows(IllegalArgumentException.class, () -> Flow.parse("swift"));
    }

    @Test
    void parsesDurations() {
        assertEquals(500, FlinkReconcilerConfig.parseDurationMs("X", "500ms"));
        assertEquals(30_000, FlinkReconcilerConfig.parseDurationMs("X", "30s"));
        assertEquals(600_000, FlinkReconcilerConfig.parseDurationMs("X", "10m"));
        assertEquals(3_600_000, FlinkReconcilerConfig.parseDurationMs("X", "1h"));
        assertEquals(250, FlinkReconcilerConfig.parseDurationMs("X", "250"));
        assertThrows(IllegalArgumentException.class, () -> FlinkReconcilerConfig.parseDurationMs("X", "10x"));
    }
}
