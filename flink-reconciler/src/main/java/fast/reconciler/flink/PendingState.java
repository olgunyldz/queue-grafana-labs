package fast.reconciler.flink;

/** POJO state for one transaction_id. */
public class PendingState {
    public SideState send;
    public SideState outcome;
    /** Whether the send side has crossed the stuck threshold and is still waiting. */
    public boolean stuck;
    public long stuckTimerAt;
    public long ttlTimerAt;

    public PendingState() {
    }
}
