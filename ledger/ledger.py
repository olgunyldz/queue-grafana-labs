"""Mock account ledger for the transfer saga.

Consumes saga commands from `ledger_commands` and answers on `ledger_results`:

    BLOCK   -> BLOCKED  or BLOCK_REJECTED (insufficient balance, chosen at random)
    SETTLE  -> SETTLED  (the blocked amount leaves the account)
    RELEASE -> RELEASED (the blocked amount goes back to the account)

Commands are idempotent: the answer to a repeated command is the first answer. The memory of answers is
lost on restart, so a command repeated after a restart is answered again; the saga ignores such late
answers (see SagaFunction conflicts).

Environment: KAFKA_BROKERS (kafka:9092), BLOCK_REJECT_RATE (0.1), LEDGER_LATENCY_MS (5..20).
"""

import json
import os
import random
import signal
import sys
import time

from confluent_kafka import Consumer, Producer

BROKERS = os.environ.get("KAFKA_BROKERS", "kafka:9092")
REJECT_RATE = float(os.environ.get("BLOCK_REJECT_RATE", "0.1"))
LATENCY_MS = os.environ.get("LEDGER_LATENCY_MS", "5-20").split("-")
COMMANDS = "ledger_commands"
RESULTS = "ledger_results"

ANSWERS = {"SETTLE": "SETTLED", "RELEASE": "RELEASED"}

answered = {}  # (transaction_id, command) -> answer event
running = True


def log(message):
    print(time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "ledger:", message, flush=True)


def answer_for(command, transaction_id):
    key = (transaction_id, command)
    if key in answered:
        return answered[key], True
    if command == "BLOCK":
        result = "BLOCK_REJECTED" if random.random() < REJECT_RATE else "BLOCKED"
    else:
        result = ANSWERS.get(command)
    if result is None:
        return None, False
    answered[key] = result
    return result, False


def on_delivery(err, msg):
    if err is not None:
        log(f"delivery failed for {msg.key()}: {err}")


def main():
    global running
    signal.signal(signal.SIGTERM, lambda *_: globals().__setitem__("running", False))

    consumer = Consumer({
        "bootstrap.servers": BROKERS,
        "group.id": "ledger",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    producer = Producer({"bootstrap.servers": BROKERS, "enable.idempotence": True, "acks": "all"})
    consumer.subscribe([COMMANDS])
    log(f"started; kafka={BROKERS} reject_rate={REJECT_RATE}")

    while running:
        msg = consumer.poll(1.0)
        if msg is None:
            producer.poll(0)
            continue
        if msg.error():
            log(f"consume error: {msg.error()}")
            continue
        try:
            command = json.loads(msg.value())
        except (TypeError, ValueError):
            log(f"skipping undecodable command at offset {msg.offset()}")
            continue
        transaction_id = command.get("transaction_id")
        name = command.get("command")
        if not transaction_id or not name:
            log(f"skipping incomplete command at offset {msg.offset()}")
            continue

        low, high = (int(v) for v in LATENCY_MS) if len(LATENCY_MS) == 2 else (5, 20)
        time.sleep(random.uniform(low, high) / 1000.0)

        result, repeated = answer_for(name, transaction_id)
        if result is None:
            log(f"unknown command {name} for {transaction_id}")
            continue
        body = json.dumps({
            "event": result,
            "transaction_id": transaction_id,
            "command": name,
            "flow": command.get("flow"),
            "repeated": repeated,
            "answered_at": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
        })
        producer.produce(RESULTS, key=transaction_id, value=body, on_delivery=on_delivery)
        producer.poll(0)
        log(f"{name} {transaction_id} -> {result}{' (repeated)' if repeated else ''}")

    producer.flush(10)
    consumer.close()
    log("stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
