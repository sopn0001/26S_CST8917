"""DEMO 2a — Service Bus: sending commands.

Note what we set on every message and why:

    message_id              enables duplicate detection AND gives the consumer
                            a natural idempotency key
    application_properties  metadata for subscription filters and tracing.
                            NEVER put routing metadata in the body — that
                            changes the contract for every consumer.
    correlation_id          follows the work across every hop

Run:
    python 02_service_bus/send_orders.py
    python 02_service_bus/send_orders.py --count 20 --big
"""
import argparse
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from azure.servicebus import ServiceBusClient, ServiceBusMessage

from shared import log
from shared.config import settings
from shared.models import Order


def build_message(order: Order, trace_id: str) -> ServiceBusMessage:
    return ServiceBusMessage(
        body=order.to_json(),
        content_type="application/json",
        # If duplicate detection is enabled on the entity, Service Bus drops
        # a repeat of this id inside the detection window.
        message_id=order.order_id,
        correlation_id=trace_id,
        subject="OrderPlaced",           # maps to sys.Label in SQL filters
        application_properties={
            "station": order.station,
            "amount": order.amount,
            "large_order": order.amount > 13.0,
        },
    )


def main(count: int, big: bool) -> None:
    log.banner(
        "Service Bus — sending orders",
        f"queue: {settings.order_queue}   ·   {count} messages",
    )

    trace_id = f"trace-{uuid.uuid4().hex[:12]}"
    log.info(f"correlation_id for this run: {trace_id}")

    with ServiceBusClient.from_connection_string(settings.servicebus_conn) as client:
        with client.get_queue_sender(settings.order_queue) as sender:
            # Batching matters: one network round trip instead of N.
            batch = sender.create_message_batch()
            for _ in range(count):
                order = Order.random()
                if big:
                    order.amount = 42.00
                msg = build_message(order, trace_id)
                try:
                    batch.add_message(msg)
                except ValueError:
                    # Batch is full — send it and start a new one.
                    sender.send_messages(batch)
                    log.sent(f"flushed a full batch")
                    batch = sender.create_message_batch()
                    batch.add_message(msg)
                log.sent(
                    f"{order.order_id}  {order.item:<9} ${order.amount:>6.2f}  "
                    f"station={order.station}"
                )
            sender.send_messages(batch)

    print()
    log.info("Now run:  python 02_service_bus/receive_peek_lock.py")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=8)
    parser.add_argument("--big", action="store_true", help="force amount > $13 to trip the filter")
    args = parser.parse_args()
    main(args.count, args.big)
