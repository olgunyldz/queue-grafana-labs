# flink/reconciler

Apache Flink reconciler for the FAST, Havale and EFT transfer flows. One code base and one image;
each flow runs as its own job in its own Flink cluster (application mode), selected with `FLOW`.

## Runtime

- Java 21, Apache Flink 2.1.3, Flink Kafka connector 5.0.0-2.1
- RocksDB keyed state by `transaction_id`, processing-time timers (30 s stuck threshold, 10 min TTL)
- Exactly-once Kafka output (transactional id prefix = `APPLICATION_ID`)

| `FLOW`   | Input topics                                          | Output topics                                          |
|----------|-------------------------------------------------------|--------------------------------------------------------|
| `fast`   | `fast_{incoming,outgoing}_{request,success,failed}`   | `fast_transfer_matched`, `fast_transfer_unmatched`     |
| `havale` | `havale_{request,success,failed}`                     | `havale_transfer_matched`, `havale_transfer_unmatched` |
| `eft`    | `eft_{incoming,outgoing}_{request,success,failed}`    | `eft_transfer_matched`, `eft_transfer_unmatched`       |

Per-flow defaults live in [Flow.java](src/main/java/fast/reconciler/flink/Flow.java). Flow-specific
business rules belong there too when the flows start to differ.

## Local Docker run

```powershell
docker compose build flink-checkpoint-init     # builds flink-reconciler:local once
docker compose up -d flink-fast-jobmanager flink-fast-taskmanager
```

Flink UI: FAST `http://localhost:18082`, Havale `:18083`, EFT `:18084`.

Each cluster exposes Prometheus metrics on port `9249`. Prometheus adds `flow` from the service
name and normalizes the operator metrics to `reconciler_*` (see `observability/prometheus.yml`).

## Configuration

Set on the JobManager service (the job's `main` runs there in application mode):

| Variable | Default | Description |
|---|---|---|
| `FLOW` | `fast` | `fast`, `havale` or `eft`; selects the defaults below |
| `KAFKA_BROKERS` | `kafka:9092` | Kafka bootstrap servers |
| `APPLICATION_ID` | `flink-<flow>-reconciler` | Job name, consumer group, transactional id prefix |
| `SEND_TOPIC`, `SUCCESS_TOPIC`, `FAILED_TOPIC` | per flow | Comma-separated input topics |
| `MATCHED_TOPIC`, `UNMATCHED_TOPIC` | `<flow>_transfer_matched` / `_unmatched` | Output topics |
| `PENDING_THRESHOLD` | `30s` | Stuck threshold |
| `MATCH_TTL` | `10m` | Unmatched TTL; matched ids are also remembered this long to count late duplicates |
| `CHECKPOINT_INTERVAL` | `10s` | Checkpoint interval |
| `PARALLELISM` | `3` | Job parallelism (each TaskManager has 4 slots) |

Durations accept `500ms`, `30s`, `10m`, `1h` or plain milliseconds; an invalid value fails the job at start.

## Restart from checkpoint

The JobManager entrypoint [docker/start-job.sh](docker/start-job.sh) starts the job from the flow's newest
completed checkpoint (`/opt/flink/checkpoints/<flow>/<job-id>/chk-N`, NO_CLAIM mode) and deletes the
checkpoint directories of older runs. Without it, a restarted JobManager (no HA) would start the job
with empty state. Set `RESTORE_FROM_CHECKPOINT=false` to start with empty state, e.g. after an
incompatible state change.

## Tests

```powershell
docker run --rm -v "${PWD}:/workspace" -w /workspace maven:3.9.15-eclipse-temurin-21 mvn -B test
```
