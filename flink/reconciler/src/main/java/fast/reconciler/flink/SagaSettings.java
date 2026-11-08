package fast.reconciler.flink;

import java.io.Serializable;

/**
 * Saga timing, from the environment:
 * <ul>
 *   <li>SAGA_COMMAND_TIMEOUT (default 30s): no ledger answer within this time -> the command is sent again</li>
 *   <li>SAGA_MAX_ATTEMPTS (default 3): sends per command before the transfer goes to MANUAL_REVIEW</li>
 *   <li>SAGA_OUTCOME_TIMEOUT (default 10m): blocked, but no FAST outcome within this time -> MANUAL_REVIEW</li>
 * </ul>
 */
record SagaSettings(long commandTimeoutMs, int maxAttempts, long outcomeTimeoutMs) implements Serializable {
    private static final long serialVersionUID = 1L;

    static SagaSettings defaults() {
        return new SagaSettings(30_000, 3, 600_000);
    }

    static SagaSettings fromEnvironment() {
        String attempts = System.getenv("SAGA_MAX_ATTEMPTS");
        return new SagaSettings(
                FlinkReconcilerConfig.durationMs("SAGA_COMMAND_TIMEOUT", "SAGA_COMMAND_TIMEOUT_MS", 30_000),
                attempts == null || attempts.isBlank() ? 3 : Integer.parseInt(attempts.trim()),
                FlinkReconcilerConfig.durationMs("SAGA_OUTCOME_TIMEOUT", "SAGA_OUTCOME_TIMEOUT_MS", 600_000));
    }
}
