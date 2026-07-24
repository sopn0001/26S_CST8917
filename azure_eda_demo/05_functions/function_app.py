"""DEMO 5 — All four services as Azure Functions triggers.

This is the same demo, but with the plumbing removed. You do not write a
receive loop, a lock renewal, a checkpoint call or an HTTP listener — the
Functions host does it. What you still own is the part that matters:
idempotency, error handling, and deciding what a failure means.

Python v2 programming model (decorators, no function.json).

Local run:
    cd 05_functions
    cp local.settings.json.example local.settings.json   # fill in connections
    func start

Deploy:
    func azure functionapp publish <your-function-app-name>
"""
import json
import logging
from typing import List

import azure.functions as func

app = func.FunctionApp()

# In production this is a table with a unique constraint on the message id,
# or a Redis SET with a TTL — written in the same transaction as the business
# change. A module-level set only survives one worker instance; it is here to
# show the shape, not to be copied into production.
_PROCESSED: set[str] = set()


# ---------------------------------------------------------------------------
# 1. SERVICE BUS QUEUE — a command that must not be lost
# ---------------------------------------------------------------------------
@app.service_bus_queue_trigger(
    arg_name="msg",
    queue_name="orders",
    connection="ServiceBusConnection",
)
def handle_order(msg: func.ServiceBusMessage) -> None:
    """Peek-lock is automatic: returning normally completes the message,
    raising abandons it. Past MaxDeliveryCount the host dead-letters it."""
    order = json.loads(msg.get_body().decode("utf-8"))
    key = msg.message_id or order["order_id"]

    logging.info(
        "order received id=%s delivery=%s correlation=%s",
        key, msg.delivery_count, msg.correlation_id,
    )

    if key in _PROCESSED:
        # Redelivery. Return normally so the message is completed — do NOT
        # raise, or you will loop until it dead-letters.
        logging.info("order %s already processed, skipping work", key)
        return

    if order.get("item") == "ramen":
        # Raising abandons the message. delivery_count increments and the
        # host retries. After MaxDeliveryCount it goes to the DLQ with the
        # exception text as the error description.
        raise RuntimeError("wok station printer offline")

    logging.info("order %s routed to the %s station", key, order["station"])
    _PROCESSED.add(key)


# ---------------------------------------------------------------------------
# 2. EVENT HUBS — a high-volume stream, delivered in batches
# ---------------------------------------------------------------------------
@app.event_hub_message_trigger(
    arg_name="events",
    event_hub_name="telemetry",
    connection="EventHubConnection",
    consumer_group="$Default",
    cardinality="many",
)
def handle_telemetry(events: List[func.EventHubEvent]) -> None:
    """cardinality='many' is what you want for a stream — one invocation per
    batch, not per event. The host checkpoints after the batch returns
    successfully, so a crash mid-batch reprocesses the whole batch."""
    hot = 0
    for event in events:
        reading = json.loads(event.get_body().decode("utf-8"))
        if reading["metric"] == "oil_temp_c" and reading["value"] > 190:
            hot += 1
            logging.warning(
                "fryer %s over temperature: %s C",
                reading["device_id"], reading["value"],
            )

    logging.info("processed telemetry batch size=%d alerts=%d", len(events), hot)


# ---------------------------------------------------------------------------
# 3. EVENT GRID — a discrete reaction, pushed to us
# ---------------------------------------------------------------------------
@app.event_grid_trigger(arg_name="event")
def handle_menu_upload(event: func.EventGridEvent) -> None:
    """Event Grid pushes; there is no queue behind this. A non-2xx response
    (i.e. an unhandled exception) triggers the retry schedule, and after
    24 hours the event lands in the dead-letter container."""
    payload = event.get_json()

    logging.info(
        "menu file event type=%s subject=%s blob=%s",
        event.event_type, event.subject, payload.get("blobUrl"),
    )

    if not payload.get("blobUrl"):
        # Do not raise on a permanently bad event — you will just burn 24
        # hours of retries on something that will never succeed. Log it,
        # emit a metric, and return.
        logging.error("event %s has no blobUrl, dropping", event.id)
        return

    # Claim check: the event carried a pointer, not the file. Fetch it now.
    logging.info("reloading menu cache from %s", payload["blobUrl"])


# ---------------------------------------------------------------------------
# 4. STORAGE QUEUE — cheap background work
# ---------------------------------------------------------------------------
@app.queue_trigger(
    arg_name="msg",
    queue_name="receipts",
    connection="AzureWebJobsStorage",
)
def handle_receipt(msg: func.QueueMessage) -> None:
    """The host manages the visibility timeout and renews it while you work.
    After maxDequeueCount (host.json, default 5) it copies the message to
    '<queue>-poison' — the closest thing Storage Queues have to a DLQ."""
    order = json.loads(msg.get_body().decode("utf-8"))

    logging.info(
        "receipt job order=%s dequeue_count=%s",
        order["order_id"], msg.dequeue_count,
    )

    if msg.dequeue_count and msg.dequeue_count > 3:
        logging.warning("order %s near the poison threshold", order["order_id"])

    logging.info("receipt generated for %s ($%.2f)", order["order_id"], order["amount"])
