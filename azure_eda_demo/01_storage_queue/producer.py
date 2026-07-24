"""DEMO 1a — Storage Queues: send.

The simplest thing that works. A queue in a storage account, over REST.
No broker features. No AMQP. Put a message in, take a message out.

Run:
    python 01_storage_queue/producer.py
    python 01_storage_queue/producer.py --count 20
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from azure.storage.queue import QueueClient

from shared import log
from shared.config import settings
from shared.models import Order


def main(count: int) -> None:
    log.banner(
        "Storage Queues — producer",
        f"queue: {settings.receipt_queue}   ·   sending {count} receipt jobs",
    )

    client = QueueClient.from_connection_string(
        settings.storage_conn, settings.receipt_queue
    )
    # create_queue is idempotent-ish: it raises if the queue already exists.
    try:
        client.create_queue()
        log.info(f"created queue '{settings.receipt_queue}'")
    except Exception:
        log.info(f"queue '{settings.receipt_queue}' already exists")

    for _ in range(count):
        order = Order.random()
        # 64 KB limit. If your payload is bigger, use the claim-check pattern:
        # put the blob in storage and send the URI instead.
        client.send_message(order.to_json())
        log.sent(f"{order.order_id}  {order.item:<9} ${order.amount:>6.2f}")

    props = client.get_queue_properties()
    log.info(f"approximate queue depth is now {props.approximate_message_count}")
    print()
    log.info("Now run:  python 01_storage_queue/consumer.py")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=5)
    main(parser.parse_args().count)
