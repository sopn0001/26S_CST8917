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

from azure.identity import DefaultAzureCredential
from azure.storage.queue import QueueClient

from shared import log
from shared.config import settings
from shared.models import Order


def main(count: int) -> None:
    log.banner(
        "Storage Queues — producer",
        f"queue: {settings.receipt_queue}   ·   sending {count} receipt jobs",
    )

    # Sign in with your local Azure identity (az login). No keys in code.
    # exclude_managed_identity_credential keeps it on your az-login user,
    # not the VM's managed identity.
    client = QueueClient(
        account_url=settings.storage_account_url,
        queue_name=settings.receipt_queue,
        credential=DefaultAzureCredential(exclude_managed_identity_credential=True),
    )

    # Put each order on the queue. That's the whole producer.
    for _ in range(count):
        order = Order.random()
        client.send_message(order.to_json())
        log.sent(f"{order.order_id}  {order.item:<9} ${order.amount:>6.2f}")

    print()
    log.info("Now run:  python 01_storage_queue/consumer.py")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=5)
    main(parser.parse_args().count)
