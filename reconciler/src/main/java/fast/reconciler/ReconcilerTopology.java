package fast.reconciler;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.Stores;

/**
 * <pre>
 * fast_send ─────────┐
 * fast_send_success ─┼─► repartition by key (transaction_id) ─► ReconcilerProcessor ─┬─► fast_transfer_matched
 * fast_send_failed ──┘        (fast-reconciler-by-transaction)   [state: RocksDB +    └─► fast_transfer_unmatched
 *                                                                  changelog topic]
 * </pre>
 *
 * The explicit repartition makes the join independent of how the producers partition their records:
 * whatever partitioner a producer uses, all events of one transaction_id meet in the same task.
 */
final class ReconcilerTopology {
    private ReconcilerTopology() {
    }

    static Topology build(Config config, ReconcilerMetrics metrics) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(ReconcilerProcessor.STORE), Serdes.String(), Serdes.String()));

        KStream<String, String> events = builder
                .stream(config.inputTopics(), Consumed.with(Serdes.String(), Serdes.String()))
                .filter((key, value) -> key != null && value != null)
                .repartition(Repartitioned.<String, String>as("by-transaction")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String())
                        .withNumberOfPartitions(config.partitions()));

        events.process(() -> new ReconcilerProcessor(config, metrics), ReconcilerProcessor.STORE)
                .to((key, value, recordContext) -> {
                    Header header = recordContext.headers().lastHeader(ReconcilerProcessor.RESULT_HEADER);
                    String result = header == null ? "" : new String(header.value(), StandardCharsets.UTF_8);
                    return "unmatched".equals(result) ? config.unmatchedTopic() : config.matchedTopic();
                }, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }
}
