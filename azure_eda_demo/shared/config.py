"""Central configuration. Everything is read from the environment.

Copy .env.example to .env, fill it in, and run any script with:

    python -m dotenv run -- python 01_storage_queue/producer.py

or just export the variables in your shell.
"""
import os
import sys

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # dotenv is optional
    pass


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        print(f"\n  Missing environment variable: {name}")
        print("  Copy .env.example to .env and fill it in, or export it in your shell.\n")
        sys.exit(1)
    return value


class Settings:
    # ---- Storage (queues + Event Hubs checkpoints + Event Grid dead letters) ----
    @property
    def storage_conn(self) -> str:
        return _require("STORAGE_CONNECTION_STRING")

    receipt_queue = os.getenv("RECEIPT_QUEUE", "receipts")
    checkpoint_container = os.getenv("CHECKPOINT_CONTAINER", "checkpoints")

    # ---- Service Bus ----
    @property
    def servicebus_conn(self) -> str:
        return _require("SERVICEBUS_CONNECTION_STRING")

    order_queue = os.getenv("ORDER_QUEUE", "orders")
    session_queue = os.getenv("SESSION_QUEUE", "orders-sessions")
    status_topic = os.getenv("STATUS_TOPIC", "order-status")
    notify_subscription = os.getenv("NOTIFY_SUBSCRIPTION", "notify")
    audit_subscription = os.getenv("AUDIT_SUBSCRIPTION", "audit")

    # ---- Event Hubs ----
    @property
    def eventhub_conn(self) -> str:
        return _require("EVENTHUB_CONNECTION_STRING")

    eventhub_name = os.getenv("EVENTHUB_NAME", "telemetry")
    consumer_group = os.getenv("EVENTHUB_CONSUMER_GROUP", "$Default")

    # ---- Event Grid ----
    @property
    def eventgrid_endpoint(self) -> str:
        return _require("EVENTGRID_TOPIC_ENDPOINT")

    @property
    def eventgrid_key(self) -> str:
        return _require("EVENTGRID_TOPIC_KEY")


settings = Settings()
