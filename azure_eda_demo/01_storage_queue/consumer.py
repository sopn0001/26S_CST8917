"""DEMO 1b — Storage Queues: receive, and the visibility timeout.

THE POINT OF THIS SCRIPT
------------------------
Receiving does NOT remove the message. It hides it for `visibility_timeout`
seconds and hands you a pop receipt. Only `delete_message` removes it.

So:
    * finish and delete   -> message is gone
    * crash before delete -> message reappears and someone else gets it

That is at-least-once delivery, and it is why every handler must be idempotent.

TRY THIS DURING THE DEMO
------------------------
1. python 01_storage_queue/producer.py --count 5
2. python 01_storage_queue/consumer.py --slow
3. Hit Ctrl-C while a message is "in flight"
4. Wait for the timeout, run the consumer again — the message is back,
   with dequeue_count incremented.

Run:
    python 01_storage_queue/consumer.py
    python 01_storage_queue/consumer.py --slow --visibility 15
"""
import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from azure.storage.queue import QueueClient

from shared import log
from shared.config import settings

# Storage Queues have no built-in dead-letter queue. You enforce a poison
# threshold yourself by watching dequeue_count.
MAX_DEQUEUE_COUNT = 3


def handle(order: dict, slow: bool) -> None:
    """Pretend to generate a PDF receipt."""
    if slow:
        time.sleep(6)
    if order.get("item") == "poutine":
        # A deliberate poison message so you can watch dequeue_count climb.
        raise RuntimeError("receipt template for 'poutine' is broken")


def main(visibility: int, slow: bool) -> None:
    log.banner(
        "Storage Queues — consumer",
        f"queue: {settings.receipt_queue}   ·   visibility_timeout={visibility}s",
    )

    client = QueueClient.from_connection_string(
        settings.storage_conn, settings.receipt_queue
    )

    empty_polls = 0
    while empty_polls < 3:
        batch = client.receive_messages(
            messages_per_page=5,
            visibility_timeout=visibility,
        )
        got_any = False

        for msg in batch:
            got_any = True
            order = json.loads(msg.content)
            log.received(
                f"{order['order_id']}  attempt #{msg.dequeue_count}  "
                f"(hidden for {visibility}s)"
            )

            if msg.dequeue_count > MAX_DEQUEUE_COUNT:
                # Manual poison handling. Service Bus does this for you.
                log.dead(
                    f"{order['order_id']} exceeded {MAX_DEQUEUE_COUNT} attempts "
                    f"— moving aside and deleting"
                )
                # In real code: copy to a 'receipts-poison' queue first.
                client.delete_message(msg)
                continue

            try:
                handle(order, slow)
            except Exception as exc:
                # No delete -> the message becomes visible again automatically.
                log.fail(f"{order['order_id']} {exc} — will reappear in {visibility}s")
                continue

            # The pop receipt proves we still hold the lock. If our visibility
            # timeout already expired, this call fails — and that is correct,
            # because someone else owns the message now.
            client.delete_message(msg)
            log.done(f"{order['order_id']} receipt generated and deleted")

        if not got_any:
            empty_polls += 1
            log.info("queue empty, polling…")
            time.sleep(2)

    print()
    log.info("Three empty polls in a row — stopping.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--visibility", type=int, default=15)
    parser.add_argument(
        "--slow", action="store_true", help="sleep 6s per message so you can Ctrl-C mid-flight"
    )
    args = parser.parse_args()
    main(args.visibility, args.slow)
