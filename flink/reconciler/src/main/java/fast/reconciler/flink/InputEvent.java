package fast.reconciler.flink;

/**
 * A transfer event, parsed once in {@link KafkaInputEventDeserializer}: the fields reconciliation needs
 * plus the Kafka record timestamp (completion latency). A POJO, so Flink serializes it efficiently on
 * the keyBy shuffle.
 */
public class InputEvent {
    public String transactionId;
    public String event;
    public double amount;
    public String failureReason;
    public String senderBank;
    public String receiverBank;
    public String senderIban;
    public String receiverIban;
    public long timestamp;

    public InputEvent() {
    }

    /** Key selector for keyBy (a method reference keeps Flink's type extraction simple). */
    static String key(InputEvent input) {
        return input.transactionId;
    }
}
