package fast.reconciler.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Initial Flink job skeleton.
 *
 * <p>The reconciliation process function will be added next. Keeping source construction separate
 * from the processor makes the Kafka contract explicit and leaves the existing Kafka Streams
 * reconciler untouched during the migration.
 */
public final class FlinkReconcilerJob {
    private FlinkReconcilerJob() {
    }

    public static void main(String[] args) throws Exception {
        FlinkReconcilerConfig config = FlinkReconcilerConfig.fromEnvironment();
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(config.checkpointIntervalMs());
        environment.setParallelism(config.parallelism());

        DataStream<InputEvent> events = mergeSources(environment, config);
        DataStream<OutputRecord> outputs = events
                .filter(input -> FlinkReconciliationFunction.transactionId(input) != null)
                .keyBy(FlinkReconciliationFunction::transactionId)
                .process(new FlinkReconciliationFunction(config))
                .name("fast-reconciliation");

        outputs.sinkTo(outputSink(config)).name("fast-reconciler-kafka-sink");

        environment.execute("FAST Flink Reconciler");
    }

    private static DataStream<InputEvent> mergeSources(
            StreamExecutionEnvironment environment,
            FlinkReconcilerConfig config) {
        return environment
                .fromSource(source(config), WatermarkStrategy.noWatermarks(), "fast-reconciler-kafka-source");
    }

    private static KafkaSource<InputEvent> source(FlinkReconcilerConfig config) {
        return KafkaSource.<InputEvent>builder()
                .setBootstrapServers(config.kafkaBrokers())
                .setTopics(config.sendTopic(), config.successTopic(), config.failedTopic())
                .setGroupId(config.applicationId())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new KafkaInputEventDeserializer())
                .build();
    }

    private static KafkaSink<OutputRecord> outputSink(FlinkReconcilerConfig config) {
        Properties producerProperties = new Properties();
        producerProperties.setProperty("transaction.timeout.ms", "900000");
        return KafkaSink.<OutputRecord>builder()
                .setBootstrapServers(config.kafkaBrokers())
                .setKafkaProducerConfig(producerProperties)
                .setRecordSerializer(KafkaRecordSerializationSchema.<OutputRecord>builder()
                        .setTopicSelector(record -> record.topic)
                        .setKeySerializationSchema((SerializationSchema<OutputRecord>) record ->
                                record.key.getBytes(StandardCharsets.UTF_8))
                        .setValueSerializationSchema((SerializationSchema<OutputRecord>) record ->
                                record.value.getBytes(StandardCharsets.UTF_8))
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(config.applicationId())
                .build();
    }
}
