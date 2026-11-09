package fast.reconciler.flink;

/** POJO state for one transaction_id. */
public class PendingState {
    public SideState send;
    public SideState outcome;
    /** Whether the send side has crossed the stuck threshold and is still waiting. */
    public boolean stuck;
    /** Matched already; kept until ttlTimerAt so late duplicates are not stored as new pending entries. */
    public boolean completed;
    public long stuckTimerAt;
    public long ttlTimerAt;

    public PendingState() {
    }
}
