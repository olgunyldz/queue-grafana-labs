"""Demo traffic for the FAST, Havale and EFT flows (Python standard library only).

Sends a request event and, for most transfers, its outcome (success or failed) to the producer
load balancer. A small share of transfers gets no outcome, so the dashboards also show pending,
stuck and unmatched transfers; one bank fails more often, so the bank panels have something to show.

    python performance/demo-traffic/traffic.py                       # 30 min, 15 transfers/s, all flows
    python performance/demo-traffic/traffic.py --minutes 10 --rate 30 --flows fast,eft
    python performance/demo-traffic/traffic.py --failure-rate 0.02 --missing-rate 0   # "healthy" demo
"""

import argparse
import json
import random
import sys
import time
import urllib.error
import urllib.request

BANKS = ["00010", "00012", "00015", "00046", "00062", "00064", "00067", "00111", "00134", "00231"]
REASONS = ["INSUFFICIENT_BALANCE", "TIMEOUT", "INVALID_IBAN", "LIMIT_EXCEEDED"]
FLOWS = {
    "fast": ["/events/fast/incoming", "/events/fast/outgoing"],
    "havale": ["/events/havale"],
    "eft": ["/events/eft/incoming", "/events/eft/outgoing"],
}


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--url", default="http://127.0.0.1:8080", help="producer load balancer (default: %(default)s)")
    p.add_argument("--minutes", type=float, default=30, help="duration (default: %(default)s)")
    p.add_argument("--rate", type=int, default=15, help="transfers per second (default: %(default)s)")
    p.add_argument("--flows", default="fast,havale,eft", help="comma-separated flows (default: %(default)s)")
    p.add_argument("--failure-rate", type=float, default=0.08,
                   help="share of transfers that end with a failed outcome (default: %(default)s)")
    p.add_argument("--missing-rate", type=float, default=0.02,
                   help="share of transfers that never get an outcome (default: %(default)s)")
    p.add_argument("--bad-bank", default="00231",
                   help="sender bank with an extra 8%% failure rate; empty to disable (default: %(default)s)")
    args = p.parse_args()
    args.flows = [f.strip() for f in args.flows.split(",") if f.strip()]
    unknown = [f for f in args.flows if f not in FLOWS]
    if unknown:
        p.error(f"unknown flow(s): {', '.join(unknown)}; expected {', '.join(FLOWS)}")
    return args


class Client:
    def __init__(self, base):
        self.base = base
        self.errors = 0

    def post(self, path, body):
        req = urllib.request.Request(self.base + path, data=json.dumps(body).encode(), method="POST",
                                     headers={"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(req, timeout=5).read()
        except (urllib.error.URLError, TimeoutError) as e:
            self.errors += 1
            if self.errors <= 5 or self.errors % 100 == 0:
                print(f"error #{self.errors} {path}: {e}", file=sys.stderr)


def main():
    args = parse_args()
    client = Client(args.url.rstrip("/"))
    end = time.time() + args.minutes * 60
    sent = failed = missing = 0
    print(f"sending {args.rate}/s to {args.url} for {args.minutes:g} min, flows={','.join(args.flows)}")
    try:
        while time.time() < end:
            start = time.time()
            for _ in range(args.rate):
                flow = random.choice(args.flows)
                base = random.choice(FLOWS[flow])
                tx = f"{flow}-{int(start * 1000)}-{sent}"
                sent += 1
                amount = round(random.lognormvariate(7, 1.2), 2)
                sender, receiver = random.sample(BANKS, 2)
                client.post(base + "/request", {"transaction_id": tx, "amount": amount,
                                                "sender_bank": sender, "receiver_bank": receiver})
                r = random.random()
                if r < args.missing_rate:
                    missing += 1
                    continue
                fail_rate = args.failure_rate + (0.08 if sender == args.bad_bank else 0)
                if r < args.missing_rate + fail_rate:
                    failed += 1
                    client.post(base + "/failed", {"transaction_id": tx, "amount": amount,
                                                   "failure_reason": random.choice(REASONS)})
                else:
                    client.post(base + "/success", {"transaction_id": tx, "amount": amount})
            time.sleep(max(0.0, 1 - (time.time() - start)))
    except KeyboardInterrupt:
        pass
    print(f"sent {sent} transfers ({failed} failed, {missing} without outcome), {client.errors} HTTP errors")


if __name__ == "__main__":
    main()
