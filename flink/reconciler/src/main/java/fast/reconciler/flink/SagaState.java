package fast.reconciler.flink;

/** Keyed saga state of one transfer (a POJO, so Flink serializes it without Kryo). */
public class SagaState {
    /** One of the SagaFunction state names. */
    public String state;
    public double amount;
    public String senderBank;
    public String receiverBank;
    public String senderIban;
    public String receiverIban;
    /** Outcome from FAST ("success" / "failed") that arrived before the block was confirmed. */
    public String fastOutcome;
    public String failureReason;
    /** Sends of the current command (1 = first send). */
    public int attempts;
    /** Processing time of the timer that is currently meaningful (stale timers are ignored). */
    public long timerAt;
    /** Processing time at which a finished saga is removed from state. */
    public long clearAt;
    public long createdAt;
    /** Processing time at which the ledger confirmed the block (0 until then). */
    public long blockedAt;
    /** The transaction_id this saga belongs to (the key; needed to track the open sagas). */
    public String transactionId;

    public SagaState() {
    }
}
