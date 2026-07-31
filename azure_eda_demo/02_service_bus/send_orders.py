"""DEMO 2a — Service Bus: send.

The simplest thing that works. A queue on a Service Bus namespace, over AMQP.
Put a message in, take a message out.

Run:
    python 02_service_bus/send_orders.py
    python 02_service_bus/send_orders.py --count 20
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from azure.identity import DefaultAzureCredential
from azure.servicebus import ServiceBusClient, ServiceBusMessage

from shared import log
from shared.config import settings
from shared.models import Order


def main(count: int) -> None:
    log.banner(
        "Service Bus — producer",
        f"queue: {settings.order_queue}   ·   sending {count} orders",
    )

    # Sign in with your local Azure identity (az login). No keys in code —
    # the namespace has local (SAS) auth disabled, so Azure AD is required.
    client = ServiceBusClient(
        fully_qualified_namespace=settings.servicebus_namespace,
        credential=DefaultAzureCredential(exclude_managed_identity_credential=True),
    )
    with client:
        with client.get_queue_sender(settings.order_queue) as sender:
            # Put each order on the queue. That's the whole producer.
            for _ in range(count):
                order = Order.random()
                sender.send_messages(ServiceBusMessage(order.to_json()))
                log.sent(f"{order.order_id}  {order.item:<9} ${order.amount:>6.2f}")

    print()
    log.info("Now run:  python 02_service_bus/receive_peek_lock.py")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=5)
    main(parser.parse_args().count)
