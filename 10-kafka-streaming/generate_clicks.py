"""Continuous synthetic clickstream producer for the Kafka streaming practice.

Emits JSON events of the form
    {"user_id": "user_017", "page": "/products", "ts": "2026-04-27T10:30:00.123"}
to a Kafka topic at a configurable rate. Designed to run for the duration of
the class so notebooks always have fresh data to consume.

Behavioural seeding (so windowed aggregations and watermarks have something
interesting to display):
- Users are Zipf-distributed: a few are very active, most are quiet
- Pages are weighted: '/' and '/products' dominate, '/checkout' is rare
- ~3% of events are emitted with a "late" timestamp (10-90 s in the past)
- ~1% of events drop the user_id (null) so quality-check exercises have signal

Run inside the jupyter container:
    docker compose exec jupyter python generate_clicks.py
or with overrides:
    docker compose exec jupyter python generate_clicks.py --rate 20 --topic clicks
"""

import argparse
import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

PAGES = [
    ("/",            0.30),
    ("/products",    0.25),
    ("/products/42", 0.10),
    ("/products/17", 0.08),
    ("/search",      0.10),
    ("/cart",        0.07),
    ("/login",       0.05),
    ("/profile",     0.03),
    ("/checkout",    0.02),
]

NUM_USERS = 50


def zipf_user_pool(n: int, alpha: float = 1.2) -> list[tuple[str, float]]:
    """Return [(user_id, weight)] with Zipf-distributed weights."""
    raw = [1.0 / (i ** alpha) for i in range(1, n + 1)]
    total = sum(raw)
    return [(f"user_{i:03d}", w / total) for i, w in enumerate(raw, start=1)]


def weighted_choice(pairs):
    keys, weights = zip(*pairs)
    return random.choices(keys, weights=weights, k=1)[0]


def make_event(users) -> dict:
    user_id = weighted_choice(users)
    page    = weighted_choice(PAGES)

    now = datetime.now(timezone.utc)
    # ~3% of events have a late event-time timestamp (sensor-style delay)
    if random.random() < 0.03:
        ts = now - timedelta(seconds=random.randint(10, 90))
    else:
        ts = now

    event = {
        "user_id": user_id,
        "page":    page,
        "ts":      ts.isoformat(timespec="milliseconds"),
        "event_id": str(uuid.uuid4()),
    }

    # ~1% of events have a missing user_id (quality-check fodder)
    if random.random() < 0.01:
        event["user_id"] = None

    return event


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9094"),
        help="Kafka bootstrap servers (default: $KAFKA_BOOTSTRAP or kafka:9094)",
    )
    p.add_argument("--topic", default="clicks", help="Topic to produce to (default: clicks)")
    p.add_argument("--rate",  type=float, default=10.0, help="Events per second (default: 10)")
    p.add_argument("--total", type=int, default=0, help="Stop after N events (0 = run forever)")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    # Connect with retries, in case Kafka is still booting
    for attempt in range(1, 11):
        try:
            producer = KafkaProducer(
                bootstrap_servers=args.bootstrap.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
                linger_ms=20,
                acks="all",
            )
            break
        except NoBrokersAvailable:
            print(f"[{attempt}/10] Kafka not ready yet, retrying in 2s...", flush=True)
            time.sleep(2)
    else:
        print("Could not connect to Kafka", file=sys.stderr)
        sys.exit(1)

    users = zipf_user_pool(NUM_USERS)
    interval = 1.0 / args.rate
    sent = 0
    stop = False

    def handle_sigint(signum, frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigint)

    print(
        f"Producing to topic={args.topic!r} at {args.rate}/s "
        f"(bootstrap={args.bootstrap}). Ctrl-C to stop.",
        flush=True,
    )

    next_tick = time.monotonic()
    try:
        while not stop and (args.total == 0 or sent < args.total):
            event = make_event(users)
            # Use user_id as the partition key so events for the same user
            # land on the same partition (preserves per-user ordering).
            key = event["user_id"]
            producer.send(args.topic, key=key, value=event)
            sent += 1

            if sent % 500 == 0:
                print(f"  sent={sent:,}", flush=True)

            next_tick += interval
            sleep_for = next_tick - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)
            else:
                # Falling behind; reset the clock so we don't burst-catch-up.
                next_tick = time.monotonic()
    finally:
        producer.flush()
        producer.close()
        print(f"Stopped. Total sent: {sent:,}", flush=True)


if __name__ == "__main__":
    main()
