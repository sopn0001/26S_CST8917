"""DEMO 2c — Service Bus: the dead-letter queue.

A DLQ nobody looks at is a silent data-loss bug. Every production system
needs three things:

    1. an alert on DeadletteredMessages > 0
    2. a way to read the reason and description
    3. a replay tool, built BEFORE you need it at 2am

This script is (2) and (3).

Run:
    python 02_service_bus/inspect_dlq.py              # read only
    python 02_service_bus/inspect_dlq.py --replay     # resubmit to the main queue
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from azure.servicebus import ServiceBusClient, ServiceBusMessage, ServiceBusSubQueue

from shared import log
from shared.config import settings


def main(replay: bool) -> None:
    log.banner(
        "Service Bus — dead-letter queue",
        f"{settings.order_queue}/$DeadLetterQueue" + ("   ·   REPLAY MODE" if replay else ""),
    )

    with ServiceBusClient.from_connection_string(settings.servicebus_conn) as client:
        receiver = client.get_queue_receiver(
            settings.order_queue,
            sub_queue=ServiceBusSubQueue.DEAD_LETTER,
            max_wait_time=5,
        )
        sender = client.get_queue_sender(settings.order_queue) if replay else None

        count = 0
        with receiver:
            for msg in receiver:
                count += 1
                # These two properties are set by the broker. Log them always.
                reason = msg.dead_letter_reason
                description = msg.dead_letter_error_description

                log.dead(
                    f"{msg.message_id}  reason={reason}  deliveries={msg.delivery_count}"
                )
                log.info(f"    {description}")

                if replay and sender:
                    # You cannot "un-dead-letter" a message. You send a copy
                    # back to the main queue and complete the original.
                    resubmitted = ServiceBusMessage(
                        body=str(msg),
                        content_type=msg.content_type,
                        message_id=msg.message_id,
                        correlation_id=msg.correlation_id,
                        subject=msg.subject,
                        application_properties=dict(msg.application_properties or {}),
                    )
                    sender.send_messages(resubmitted)
                    receiver.complete_message(msg)
                    log.sent(f"    replayed {msg.message_id} to {settings.order_queue}")

        if sender:
            sender.close()

    print()
    if count == 0:
        log.info("Dead-letter queue is empty.")
    elif replay:
        log.info(f"Replayed {count} message(s). Fix the cause before you replay in production.")
    else:
        log.info(f"{count} message(s) waiting. Re-run with --replay to resubmit them.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--replay", action="store_true")
    main(parser.parse_args().replay)
