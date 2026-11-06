package fast.reconciler.flink;

import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/** Preserves the Kafka record timestamp needed for completion latency and TTL decisions. */
final class KafkaInputEventDeserializer implements KafkaRecordDeserializationSchema<InputEvent> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<InputEvent> out) {
        if (record.value() != null) {
            out.collect(new InputEvent(
                    new String(record.value(), StandardCharsets.UTF_8),
                    record.timestamp()));
        }
    }

    @Override
    public TypeInformation<InputEvent> getProducedType() {
        return TypeInformation.of(InputEvent.class);
    }
}
