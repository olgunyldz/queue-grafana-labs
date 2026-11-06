package fast.reconciler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReconcilerTopologyTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Instant T0 = Instant.parse("2026-11-04T10:00:00Z");

    private TopologyTestDriver driver;
    private SimpleMeterRegistry registry;
    private TestInputTopic<String, String> send;
    private TestInputTopic<String, String> success;
    private TestInputTopic<String, String> failed;
    private TestOutputTopic<String, String> matched;
    private TestOutputTopic<String, String> unmatched;

    @BeforeEach
    void setUp() throws Exception {
        Config config = new Config("dummy:9092", "fast-reconciler-test",
                List.of("fast_send", "fast_send_success", "fast_send_failed"),
                "fast_transfer_matched", "fast_transfer_unmatched", 3,
                Duration.ofSeconds(30), Duration.ofMinutes(10), Duration.ofSeconds(5),
                0, "at_least_once", Files.createTempDirectory("fast-reconciler").toString(), 0);
        registry = new SimpleMeterRegistry();
        Properties props = Main.streamsProperties(config);
        driver = new TopologyTestDriver(ReconcilerTopology.build(config, new ReconcilerMetrics(registry)), props, T0);
        StringSerializer ser = new StringSerializer();
        StringDeserializer de = new StringDeserializer();
        send = driver.createInputTopic("fast_send", ser, ser);
        success = driver.createInputTopic("fast_send_success", ser, ser);
        failed = driver.createInputTopic("fast_send_failed", ser, ser);
        matched = driver.createOutputTopic("fast_transfer_matched", de, de);
        unmatched = driver.createOutputTopic("fast_transfer_unmatched", de, de);
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    private static String event(String name, String tx, double amount, String extra) {
        return "{\"event\":\"" + name + "\",\"transaction_id\":\"" + tx + "\",\"payload\":{\"transaction_id\":\"" + tx
                + "\",\"amount\":" + amount + extra + "}}";
    }

    @Test
    void matchesSendThenSuccessAndTakesBankFromSend() throws Exception {
        send.pipeInput("tx-1", event("fast_send", "tx-1", 1250.75, ",\"sender_bank\":\"00010\",\"receiver_bank\":\"00046\""), T0);
        // The outcome carries no bank or amount: they must come from fast_send.
        success.pipeInput("tx-1", event("fast_send_success", "tx-1", 1250.75, ""), T0.plusMillis(800));

        JsonNode out = JSON.readTree(matched.readValue());
        assertEquals("tx-1", out.get("transaction_id").asText());
        assertEquals("fast_send_success", out.get("outcome").asText());
        assertEquals(800, out.get("completion_ms").asLong());
        assertEquals("Ziraat Bankas\u0131", out.get("sender_bank_name").asText());
        assertEquals("Akbank", out.get("receiver_bank_name").asText());
        assertTrue(unmatched.isEmpty());
    }

    @Test
    void matchesOutcomeThatArrivesBeforeSend() throws Exception {
        failed.pipeInput("tx-2", event("fast_send_failed", "tx-2", 99.9, ",\"failure_reason\":\"INSUFFICIENT_BALANCE\""), T0.plusSeconds(2));
        send.pipeInput("tx-2", event("fast_send", "tx-2", 99.9, ",\"sender_bank\":\"00231\""), T0);

        JsonNode out = JSON.readTree(matched.readValue());
        assertEquals("fast_send_failed", out.get("outcome").asText());
        assertEquals(2000, out.get("completion_ms").asLong());
        assertEquals("INSUFFICIENT_BALANCE", out.get("failure_reason").asText());
        assertEquals("Papara", out.get("sender_bank_name").asText());
    }

    @Test
    void reportsStuckAndExpiresSendWithoutOutcome() throws Exception {
        send.pipeInput("tx-3", event("fast_send", "tx-3", 10, ""), T0);

        driver.advanceWallClockTime(Duration.ofSeconds(40));
        assertEquals(1.0, registry.get("fast.kstreams.reconciler.stuck").gauge().value());
        assertTrue(unmatched.isEmpty());

        driver.advanceWallClockTime(Duration.ofMinutes(10));
        JsonNode out = JSON.readTree(unmatched.readValue());
        assertEquals("send_without_outcome", out.get("kind").asText());
        assertEquals(0.0, registry.get("fast.kstreams.reconciler.pending").gauge().value());
        assertEquals(1.0, registry.get("fast.kstreams.reconciler.unmatched").tag("kind", "send_without_outcome").counter().count());
        assertTrue(matched.isEmpty());
    }

    @Test
    void countsDuplicates() {
        send.pipeInput("tx-4", event("fast_send", "tx-4", 10, ""), T0);
        send.pipeInput("tx-4", event("fast_send", "tx-4", 10, ""), T0.plusMillis(5));
        assertEquals(1.0, registry.get("fast.kstreams.reconciler.duplicates").tag("event", "fast_send").counter().count());
    }
}
