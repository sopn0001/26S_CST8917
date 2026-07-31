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
    # ---- Storage Queues ----
    @property
    def storage_account_url(self) -> str:
        return _require("STORAGE_ACCOUNT_URL")

    receipt_queue = os.getenv("RECEIPT_QUEUE", "receipts")

    # ---- Service Bus ----
    @property
    def servicebus_namespace(self) -> str:
        return _require("SERVICEBUS_NAMESPACE")

    order_queue = os.getenv("ORDER_QUEUE", "orders")

    # ---- Event Hubs ----
    @property
    def eventhub_namespace(self) -> str:
        return _require("EVENTHUB_NAMESPACE")

    eventhub_name = os.getenv("EVENTHUB_NAME", "telemetry")
    consumer_group = os.getenv("EVENTHUB_CONSUMER_GROUP", "$Default")


settings = Settings()
