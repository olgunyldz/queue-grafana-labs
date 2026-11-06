# FAST Flink Reconciler

This folder contains the parallel Apache Flink implementation of the FAST transfer reconciler. The existing Kafka Streams implementation in `../reconciler/` is preserved.

## Runtime

- Java 21
- Apache Flink 2.1.3
- Flink Kafka connector 5.0.0-2.1
- Kafka topics: `fast_send`, `fast_send_success`, `fast_send_failed`
- Output topics: `fast_transfer_matched`, `fast_transfer_unmatched`

The job uses keyed Flink state by `transaction_id`, processing-time timers for the 30-second stuck threshold and 10-minute unmatched TTL, RocksDB state, and exactly-once Kafka output transactions.

## Local Docker run

Build and start the existing stack with the Flink services:

```powershell
docker compose build flink-jobmanager
docker compose up -d flink-jobmanager flink-taskmanager flink-reconciler-submit
```

Flink UI: `http://localhost:18082`

Prometheus scrapes the Flink task manager reporter on port `9249`. The custom metric group is `fast_reconciler` and exposes counters/gauges for transfers, unmatched records, duplicates, invalid events, pending records, stuck transfers, and orphan outcomes.

## Configuration

The job reads these environment variables:

- `KAFKA_BROKERS` (default `kafka:9092`)
- `APPLICATION_ID` (default `fast-flink-reconciler`)
- `FAST_PENDING_THRESHOLD` (default `30s`)
- `FAST_MATCH_TTL` (default `10m`)
- `CHECKPOINT_INTERVAL` (default `10s`)
- `PARALLELISM` (default `3`)

The unit tests cover send/outcome ordering, matched output, and TTL expiration. They run with:

```powershell
docker run --rm -v "${PWD}:/workspace" -w /workspace maven:3.9.15-eclipse-temurin-21 mvn -B test
```
