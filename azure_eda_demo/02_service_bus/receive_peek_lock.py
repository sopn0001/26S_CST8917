"""DEMO 2b — Service Bus: peek-lock and lock renewal.

Receiving in peek-lock mode does NOT remove the message — it locks it for
`lock_duration` seconds (30s by default). If your work runs longer than that
window, the lock expires and the broker redelivers the message to another
consumer — now it runs twice. The fix is to renew the lock: call
`renew_message_lock` before the current lock expires.

    peek     — look at the next message without locking it
    receive  — lock it, hidden from other consumers, with a lock token
    process  — pretend to work for 9s
    renew    — renew the lock so it never reappears
    complete — remove it for good

Run:
    python 02_service_bus/send_orders.py --count 1
    python 02_service_bus/receive_peek_lock.py
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from azure.identity import DefaultAzureCredential
from azure.servicebus import ServiceBusClient

from shared import log
from shared.config import settings


def main() -> None:
    log.banner(
        "Service Bus — peek-lock & lock renewal",
        f"queue: {settings.order_queue}",
    )

    # Sign in with your local Azure identity (az login). No keys in code —
    # the namespace has local (SAS) auth disabled, so Azure AD is required.
    client = ServiceBusClient(
        fully_qualified_namespace=settings.servicebus_namespace,
        credential=DefaultAzureCredential(exclude_managed_identity_credential=True),
    )
    with client:
        # 1. PEEK — look at the next message. It stays available for others.
        with client.get_queue_receiver(settings.order_queue) as peeker:
            peeked = peeker.peek_messages(max_message_count=1)
            if not peeked:
                log.info("queue is empty — run the producer first")
                return
            log.info(f"peeked:   {str(peeked[0])}")

        # 2. RECEIVE — lock it. Hidden from other consumers until we complete.
        with client.get_queue_receiver(
            settings.order_queue, max_wait_time=5
        ) as receiver:
            msgs = receiver.receive_messages(max_message_count=1, max_wait_time=5)
            if not msgs:
                log.info("nothing to receive right now")
                return
            msg = msgs[0]
            log.received(f"received: {str(msg)}  (locked)")

            # 3. PROCESS — pretend the work takes 9s.
            log.info("processing… (9s of work under the lock)")
            time.sleep(9)

            # 4. RENEW — renew the lock before it expires, so the message
            #    never reappears for a competing consumer.
            receiver.renew_message_lock(msg)
            log.info("lock renewed")
            time.sleep(1)  # a little more work under the renewed lock

            # 5. COMPLETE — work is done, remove it for good.
            receiver.complete_message(msg)
            log.done("completed")


if __name__ == "__main__":
    main()
