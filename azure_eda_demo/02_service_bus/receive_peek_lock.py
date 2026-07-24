"""DEMO 2b — Service Bus: peek-lock and the three ways a message ends.

THE THREE ENDINGS
-----------------
    complete_message()      done. Removed from the queue.
    abandon_message()       give up the lock immediately, redeliver now.
                            delivery_count increments.
    dead_letter_message()   stop trying. Move to the DLQ with a reason.

If you do none of these, the lock expires and the broker redelivers anyway.
Service Bus counts deliveries for you: past MaxDeliveryCount (default 10) it
dead-letters automatically. Storage Queues make you do that yourself.

WATCH FOR
---------
* delivery_count climbing on the 'ramen' orders (they fail on purpose)
* the SKIPPED lines — that is idempotency working on a redelivered message
* run 02_service_bus/inspect_dlq.py afterwards

Run:
    python 02_service_bus/receive_peek_lock.py
    python 02_service_bus/receive_peek_lock.py --workers 3     # competing consumers
"""
import argparse
import json
import random
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from azure.servicebus import ServiceBusClient
from azure.servicebus.exceptions import MessageLockLostError

from shared import log
from shared.config import settings

# Shared across the worker threads: the same guard a real consumer needs.
SEEN = log.SeenStore()
SEEN_LOCK = threading.Lock()

POISON_ITEM = "ramen"


def process(order: dict) -> None:
    """Business logic. Fails deterministically for one item."""
    time.sleep(random.uniform(0.2, 0.6))
    if order["item"] == POISON_ITEM:
        raise RuntimeError("kitchen printer for the wok station is offline")


def worker(name: str, deadline: float) -> None:
    with ServiceBusClient.from_connection_string(settings.servicebus_conn) as client:
        # max_wait_time makes the iterator stop instead of blocking forever.
        with client.get_queue_receiver(
            settings.order_queue, max_wait_time=5
        ) as receiver:
            for msg in receiver:
                if time.time() > deadline:
                    break

                order = json.loads(str(msg))
                key = msg.message_id or order["order_id"]

                log.received(
                    f"[{name}] {order['order_id']}  delivery #{msg.delivery_count + 1}  "
                    f"trace={msg.correlation_id}"
                )

                # ---- idempotency guard -------------------------------------
                # At-least-once delivery means we WILL see repeats. Doing the
                # work twice must be indistinguishable from doing it once.
                with SEEN_LOCK:
                    if SEEN.already_processed(key):
                        log.skip(f"[{name}] {key} already processed — completing without redoing work")
                        receiver.complete_message(msg)
                        continue

                # ---- do the work -------------------------------------------
                try:
                    process(order)
                except Exception as exc:
                    if msg.delivery_count >= 4:
                        # We know it will never succeed. Stop burning retries.
                        receiver.dead_letter_message(
                            msg,
                            reason="UnprocessableOrder",
                            error_description=str(exc),
                        )
                        log.dead(f"[{name}] {order['order_id']} -> DLQ: {exc}")
                    else:
                        receiver.abandon_message(msg)
                        log.fail(f"[{name}] {order['order_id']} {exc} — abandoned, will retry")
                    continue

                # ---- commit ------------------------------------------------
                try:
                    receiver.complete_message(msg)
                except MessageLockLostError:
                    # The work took longer than the lock. The message is being
                    # redelivered right now — our idempotency guard covers it.
                    log.warn(f"[{name}] lock lost on {order['order_id']} — redelivery expected")
                    continue

                with SEEN_LOCK:
                    SEEN.mark(key)
                log.done(f"[{name}] {order['order_id']} sent to the {order['station']} station")


def main(workers: int, seconds: int) -> None:
    log.banner(
        "Service Bus — peek-lock receiver",
        f"queue: {settings.order_queue}   ·   {workers} competing consumer(s)   ·   {seconds}s",
    )
    deadline = time.time() + seconds

    threads = [
        threading.Thread(target=worker, args=(f"w{i + 1}", deadline), daemon=True)
        for i in range(workers)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print()
    log.info("Done. Check the dead-letter queue: python 02_service_bus/inspect_dlq.py")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=1, help="competing consumers")
    parser.add_argument("--seconds", type=int, default=45)
    args = parser.parse_args()
    main(args.workers, args.seconds)
