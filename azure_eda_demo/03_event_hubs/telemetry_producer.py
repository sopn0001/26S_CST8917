"""DEMO 3a — Event Hubs: producing a stream.

Event Hubs is NOT a queue. It is a partitioned, append-only log. Sending
appends to the end of a partition; nothing is ever removed by a reader.

THE PARTITION KEY IS THE WHOLE GAME
-----------------------------------
    same key  -> same partition -> guaranteed order
    no key    -> round-robin    -> maximum throughput, no order

Use a business identity (device id, customer id, order id). Never a random
GUID — that gives you perfect spread and zero ordering, which is almost
never what the requirement actually meant.

Watch for hot keys: if one fryer emits 90% of your events, one partition
does 90% of the work and the other three idle.

Run:
    python 03_event_hubs/telemetry_producer.py
    python 03_event_hubs/telemetry_producer.py --count 5000 --no-key
"""
import argparse
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from azure.eventhub import EventData, EventHubProducerClient

from shared import log
from shared.config import settings
from shared.models import Telemetry

DEVICES = ["fryer-1", "fryer-2", "fryer-3", "fryer-4"]


def main(count: int, use_key: bool, batch_size: int) -> None:
    log.banner(
        "Event Hubs — telemetry producer",
        f"hub: {settings.eventhub_name}   ·   {count} events   ·   "
        + ("partition_key=device_id" if use_key else "NO partition key (round-robin)"),
    )

    producer = EventHubProducerClient.from_connection_string(
        settings.eventhub_conn, eventhub_name=settings.eventhub_name
    )

    sent_per_device: Counter[str] = Counter()
    started = time.time()

    with producer:
        remaining = count
        while remaining > 0:
            device = DEVICES[remaining % len(DEVICES)]

            # A batch is bound to ONE partition when you set a partition key.
            # So build one batch per key, not one batch for everything.
            batch = (
                producer.create_batch(partition_key=device)
                if use_key
                else producer.create_batch()
            )

            added = 0
            while added < batch_size and remaining > 0:
                reading = Telemetry.random(device_id=device)
                try:
                    batch.add(EventData(reading.to_json()))
                except ValueError:
                    break          # batch is full (1 MB) — send what we have
                added += 1
                remaining -= 1
                sent_per_device[device] += 1

            producer.send_batch(batch)
            log.sent(f"{added:>4} events  key={device if use_key else '(none)':<10} "
                     f"{count - remaining}/{count}")

    elapsed = max(time.time() - started, 0.001)
    print()
    log.info(f"{count} events in {elapsed:.1f}s  (~{count / elapsed:,.0f} events/sec)")
    for device, n in sorted(sent_per_device.items()):
        log.info(f"    {device}: {n}")
    print()
    log.info("Now run:  python 03_event_hubs/telemetry_consumer.py")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=2000)
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument(
        "--no-key",
        action="store_true",
        help="send without a partition key — throughput up, ordering gone",
    )
    args = parser.parse_args()
    main(args.count, not args.no_key, args.batch_size)
