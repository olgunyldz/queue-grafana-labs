package fast.reconciler.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Matches the request and outcome events of one flow by transaction_id (keyed state).
 *
 * <p>Timers are processing-time and start when an entry is first stored, not from the Kafka record
 * timestamp. Otherwise, while a backlog is being consumed, an old request would expire immediately,
 * before its outcome (read from another topic a moment later) arrives.
 *
 * <p>After a match the key keeps a small "completed" marker for MATCH_TTL, so a late duplicate of
 * either side is counted as a duplicate instead of opening a new pending entry.
 *
 * <p>The pending / stuck / orphan gauges are checkpointed as operator state and restored on restart.
 */
final class FlinkReconciliationFunction extends KeyedProcessFunction<String, InputEvent, OutputRecord>
        implements CheckpointedFunction {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final FlinkReconcilerConfig config;
    private transient Set<String> sendEvents;
    private transient ValueState<PendingState> state;
    private transient FlinkReconciliationMetrics metrics;
    private transient ListState<long[]> gaugeState;
    /** {pending, stuck, orphanOutcomes} restored from the last checkpoint. */
    private transient long[] restoredGauges;

    FlinkReconciliationFunction(FlinkReconcilerConfig config) {
        this.config = config;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        gaugeState = context.getOperatorStateStore().getUnionListState(
                new ListStateDescriptor<>("reconciler-gauges", PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO));
        restoredGauges = new long[3];
        if (!context.isRestored()) {
            return;
        }
        int index = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        int parallelism = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
        List<long[]> entries = new ArrayList<>();
        for (long[] entry : gaugeState.get()) {
            entries.add(entry);
        }
        // Entry layout: {subtaskIndex, parallelism, pending, stuck, orphans}. With unchanged parallelism
        // each subtask takes its own values. After rescaling, keys move between subtasks, so subtask 0
        // takes the totals: per-subtask values are then approximate but their sum stays exact.
        boolean sameParallelism = !entries.isEmpty()
                && entries.stream().allMatch(entry -> entry[1] == parallelism);
        for (long[] entry : entries) {
            if (sameParallelism ? entry[0] == index : index == 0) {
                restoredGauges[0] += entry[2];
                restoredGauges[1] += entry[3];
                restoredGauges[2] += entry[4];
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        gaugeState.update(List.of(new long[] {
                getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(),
                getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks(),
                metrics.pending(),
                metrics.stuck(),
                metrics.orphanOutcomes()}));
    }

    @Override
    public void open(OpenContext parameters) {
        sendEvents = split(config.sendTopic());
        state = getRuntimeContext().getState(
                new ValueStateDescriptor<>("fast-transfer-state", PendingState.class));
        metrics = new FlinkReconciliationMetrics(getRuntimeContext().getMetricGroup(),
                split(config.failedTopic()), restoredGauges == null ? new long[3] : restoredGauges);
    }

    @Override
    public void processElement(InputEvent input, Context context, Collector<OutputRecord> out) throws Exception {
        // Parsed and validated in KafkaInputEventDeserializer (transaction_id and event are present).
        String transactionId = input.transactionId;
        String eventName = input.event;
        long timestamp = input.timestamp > 0
                ? input.timestamp
                : context.timerService().currentProcessingTime();
        PendingState current = state.value();
        if (current == null) {
            current = new PendingState();
        }

        if (current.completed) {
            metrics.duplicate(eventName);
            return;
        }

        if (sendEvents.contains(eventName)) {
            if (current.outcome != null) {
                emitMatched(transactionId, sendSide(input, timestamp), current.outcome, out);
                complete(current, context);
                metrics.matchedFromOutcome();
            } else if (current.send != null) {
                metrics.duplicate(eventName);
            } else {
                current.send = sendSide(input, timestamp);
                registerTimers(current, context);
                state.update(current);
                metrics.sendStored();
            }
            return;
        }

        SideState outcome = new SideState(
                eventName,
                timestamp,
                input.amount,
                input.failureReason,
                BankResolver.code(input.senderBank, input.senderIban),
                BankResolver.code(input.receiverBank, input.receiverIban));

        if (current.send != null) {
            emitMatched(transactionId, current.send, outcome, out);
            if (current.stuck) {
                metrics.unmarkStuck();
            }
            complete(current, context);
            metrics.matchedFromSend();
        } else if (current.outcome != null) {
            metrics.duplicate(eventName);
        } else {
            current.outcome = outcome;
            registerTimers(current, context);
            state.update(current);
            metrics.outcomeStored();
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<OutputRecord> out) throws Exception {
        PendingState current = state.value();
        if (current == null) {
            return;
        }

        if (current.completed) {
            if (timestamp == current.ttlTimerAt) {
                state.clear();
            }
            return;
        }

        if (timestamp == current.stuckTimerAt && current.send != null && current.outcome == null) {
            if (!current.stuck) {
                current.stuck = true;
                metrics.markStuck();
                state.update(current);
            }
        }

        if (timestamp != current.ttlTimerAt) {
            return;
        }

        boolean sendSide = current.send != null;
        SideState side = sendSide ? current.send : current.outcome;
        String kind = sendSide ? "send_without_outcome" : "outcome_without_send";
        ObjectNode result = JSON.createObjectNode()
                .put("transaction_id", context.getCurrentKey())
                .put("kind", kind)
                .put("event", side.event)
                .put("amount", side.amount)
                .put("sender_bank", side.senderBank)
                .put("receiver_bank", side.receiverBank)
                .put("first_seen_at", Instant.ofEpochMilli(side.timestamp).toString())
                .put("expired_at", Instant.ofEpochMilli(timestamp).toString());
        out.collect(new OutputRecord(config.unmatchedTopic(), context.getCurrentKey(), result.toString()));
        state.clear();
        if (current.stuck) {
            metrics.unmarkStuck();
        }
        metrics.unmatched(kind);
        if (sendSide) {
            metrics.unmatchedFromSend();
        } else {
            metrics.unmatchedFromOutcome();
        }
    }

    private void emitMatched(String transactionId, SideState send, SideState outcome,
            Collector<OutputRecord> out) {
        long completionMs = Math.max(0, outcome.timestamp - send.timestamp);
        String result = outcome.event;
        String senderBank = firstNonNull(send.senderBank, outcome.senderBank);
        String receiverBank = firstNonNull(send.receiverBank, outcome.receiverBank);
        String[] sender = BankResolver.labels(senderBank);
        String[] receiver = BankResolver.labels(receiverBank);
        double amount = send.amount != 0 ? send.amount : outcome.amount;

        ObjectNode output = JSON.createObjectNode()
                .put("transaction_id", transactionId)
                .put("outcome", result)
                .put("completion_ms", completionMs)
                .put("amount", amount)
                .put("currency", "TRY")
                .put("sender_bank", senderBank)
                .put("sender_bank_name", sender[1])
                .put("receiver_bank", receiverBank)
                .put("receiver_bank_name", receiver[1])
                .put("failure_reason", outcome.failureReason)
                .put("sent_at", Instant.ofEpochMilli(send.timestamp).toString())
                .put("completed_at", Instant.ofEpochMilli(outcome.timestamp).toString());
        out.collect(new OutputRecord(config.matchedTopic(), transactionId, output.toString()));
        metrics.matched(result, completionMs, amount, senderBank, receiverBank, outcome.failureReason);
    }

    /** Replaces the pending entry with a completed marker that expires after MATCH_TTL. */
    private void complete(PendingState current, Context context) throws Exception {
        if (current.ttlTimerAt != 0) {
            context.timerService().deleteProcessingTimeTimer(current.ttlTimerAt);
        }
        if (current.stuckTimerAt != 0) {
            context.timerService().deleteProcessingTimeTimer(current.stuckTimerAt);
        }
        PendingState completed = new PendingState();
        completed.completed = true;
        completed.ttlTimerAt = context.timerService().currentProcessingTime() + config.matchTtlMs();
        context.timerService().registerProcessingTimeTimer(completed.ttlTimerAt);
        state.update(completed);
    }

    /** Timers count from the moment the entry is stored (see class comment). */
    private void registerTimers(PendingState current, Context context) {
        long now = context.timerService().currentProcessingTime();
        current.ttlTimerAt = now + config.matchTtlMs();
        context.timerService().registerProcessingTimeTimer(current.ttlTimerAt);
        if (current.send != null) {
            current.stuckTimerAt = now + config.pendingThresholdMs();
            context.timerService().registerProcessingTimeTimer(current.stuckTimerAt);
        }
    }

    private static Set<String> split(String csv) {
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .collect(Collectors.toUnmodifiableSet());
    }

    private static SideState sendSide(InputEvent input, long timestamp) {
        return new SideState(
                input.event,
                timestamp,
                input.amount,
                null,
                BankResolver.code(input.senderBank, input.senderIban),
                BankResolver.code(input.receiverBank, input.receiverIban));
    }

    /** Visible for tests. */
    FlinkReconciliationMetrics metrics() {
        return metrics;
    }

    private static String firstNonNull(String first, String second) {
        return first != null ? first : second;
    }
}
