package fast.reconciler.flink;

/** Output record routed to a matched or unmatched Kafka topic. */
public class OutputRecord {
    public String topic;
    public String key;
    public String value;

    public OutputRecord() {
    }

    public OutputRecord(String topic, String key, String value) {
        this.topic = topic;
        this.key = key;
        this.value = value;
    }
}
