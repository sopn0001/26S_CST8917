"""DEMO 1b — Storage Queues: the visibility timeout and lease renewal.

A message you receive is hidden for `visibility_timeout` seconds, not removed.
If your work runs longer than that window, the message reappears and another
consumer grabs it — now it runs twice. The fix is to renew the lease: call
`update_message` with a fresh visibility timeout before the current one expires.

    peek    — look at the next message without dequeuing it
    receive — dequeue it, hidden for 10s, with a pop receipt
    process — pretend to work for 9s (nearly the whole window)
    update  — renew the lease for another 10s so it never reappears
    delete  — remove it for good

Run:
    python 01_storage_queue/producer.py --count 1
    python 01_storage_queue/consumer.py
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from azure.identity import DefaultAzureCredential
from azure.storage.queue import QueueClient

from shared import log
from shared.config import settings


def main() -> None:
    log.banner(
        "Storage Queues — visibility timeout & lease renewal",
        f"queue: {settings.receipt_queue}",
    )

    credential = DefaultAzureCredential(exclude_managed_identity_credential=True)
    client = QueueClient(
        account_url=settings.storage_account_url,
        queue_name=settings.receipt_queue,
        credential=credential,
    )

    # 1. PEEK — look at the next message. It stays visible for others.
    peeked = client.peek_messages()
    if not peeked:
        log.info("queue is empty — run the producer first")
        return
    log.info(f"peeked:   {peeked[0].content}")

    # 2. RECEIVE — dequeue it. Hidden from other consumers for 10 seconds.
    msg = client.receive_message(visibility_timeout=10)
    log.received(f"received: {msg.content}  (hidden for 10s)")

    # 3. PROCESS — pretend the work takes 9s, almost the whole 10s window.
    log.info("processing… (9s, nearly the whole visibility window)")
    time.sleep(9)

    # 4. UPDATE — renew the lease for another 10s before it expires, so the
    #    message never reappears for a competing consumer.
    msg = client.update_message(msg, visibility_timeout=10)
    log.info("lease renewed: hidden for another 10s")
    time.sleep(1)  # a little more work under the renewed lease

    # 5. DELETE — work is done, remove it for good.
    client.delete_message(msg)
    log.done("deleted")


if __name__ == "__main__":
    main()

