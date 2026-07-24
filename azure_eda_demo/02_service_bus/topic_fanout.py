"""DEMO 2d — Service Bus topics: one publish, many independent copies.

A QUEUE distributes work: each message goes to exactly one consumer.
A TOPIC distributes copies: each subscription gets its own.

The subscriptions in this demo have different rules (see infra/provision.sh):

    notify   1=1                       -> everything
    audit    amount > 13               -> only large orders

The filter runs INSIDE the broker. The audit subscription is never delivered
the small orders at all — you are not billed for them and you do not write
"if amount > 13: return" in your consumer.

Run in three terminals:
    python 02_service_bus/topic_fanout.py --listen notify
    python 02_service_bus/topic_fanout.py --listen audit
    python 02_service_bus/topic_fanout.py --publish
"""
import argparse
import json
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from azure.servicebus import ServiceBusClient, ServiceBusMessage

from shared import log
from shared.config import settings
from shared.models import Order, OrderStatusChanged

STATUSES = ["accepted", "cooking", "ready", "collected"]


def publish(count: int) -> None:
    log.banner(
        "Service Bus topic — publisher",
        f"topic: {settings.status_topic}   ·   {count} status events",
    )
    with ServiceBusClient.from_connection_string(settings.servicebus_conn) as client:
        with client.get_topic_sender(settings.status_topic) as sender:
            for _ in range(count):
                order = Order.random()
                event = OrderStatusChanged(
                    order_id=order.order_id,
                    status=random.choice(STATUSES),
                    station=order.station,
                    amount=order.amount,
                )
                sender.send_messages(
                    ServiceBusMessage(
                        body=event.to_json(),
                        content_type="application/json",
                        message_id=f"{event.order_id}-{event.status}",
                        subject="OrderStatusChanged",
                        # The filter reads THIS, not the body.
                        application_properties={
                            "amount": event.amount,
                            "station": event.station,
                        },
                    )
                )
                flag = "LARGE" if event.amount > 13 else "     "
                log.sent(f"{flag} {event.order_id}  {event.status:<10} ${event.amount:>6.2f}")

    print()
    log.info("The 'audit' subscription should only have received the LARGE ones.")


def listen(subscription: str, seconds: int) -> None:
    log.banner(
        f"Service Bus subscription — {subscription}",
        f"{settings.status_topic}/{subscription}   ·   listening {seconds}s",
    )
    with ServiceBusClient.from_connection_string(settings.servicebus_conn) as client:
        with client.get_subscription_receiver(
            settings.status_topic, subscription, max_wait_time=seconds
        ) as receiver:
            for msg in receiver:
                event = json.loads(str(msg))
                log.received(
                    f"[{subscription}] {event['order_id']}  {event['status']:<10} "
                    f"${event['amount']:>6.2f}"
                )
                receiver.complete_message(msg)

    print()
    log.info(f"'{subscription}' stopped listening.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--listen", type=str, help="subscription name: notify or audit")
    parser.add_argument("--count", type=int, default=8)
    parser.add_argument("--seconds", type=int, default=30)
    args = parser.parse_args()

    if args.publish:
        publish(args.count)
    elif args.listen:
        listen(args.listen, args.seconds)
    else:
        parser.error("pass --publish or --listen <subscription>")
