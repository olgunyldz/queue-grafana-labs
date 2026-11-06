package fast.reconciler.flink;

/** Kafka value together with the Kafka record timestamp used by reconciliation. */
public class InputEvent {
    public String value;
    public long timestamp;

    public InputEvent() {
    }

    public InputEvent(String value, long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }
}
