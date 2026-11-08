package fast.reconciler.flink;

/** Normalized send or outcome side kept in keyed state. */
public class SideState {
    public String event;
    public long timestamp;
    public double amount;
    public String failureReason;
    public String senderBank;
    public String receiverBank;

    public SideState() {
    }

    public SideState(String event, long timestamp, double amount, String failureReason,
            String senderBank, String receiverBank) {
        this.event = event;
        this.timestamp = timestamp;
        this.amount = amount;
        this.failureReason = failureReason;
        this.senderBank = senderBank;
        this.receiverBank = receiverBank;
    }
}
