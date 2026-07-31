"""DEMO 3b — Event Hubs: receive a stream.

A consumer reads from the partitioned log. Reading does NOT remove events —
the log stays put and any number of consumer groups can read it independently.

This is a live tail. Start the consumer first and wait for "reader attached"
(authenticating + connecting takes a few seconds). THEN run the producer in
another terminal and watch the events arrive. Ctrl-C to stop.

    Terminal 1:  python 03_event_hubs/telemetry_consumer.py
    Terminal 2:  python 03_event_hubs/telemetry_producer.py

Run:
    python 03_event_hubs/telemetry_consumer.py
    python 03_event_hubs/telemetry_consumer.py --seconds 30
"""
import argparse
import json
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from azure.identity import DefaultAzureCredential
from azure.eventhub import EventHubConsumerClient

from shared import log
from shared.config import settings


def main(group: str, seconds: int | None) -> None:
    log.banner(
        "Event Hubs — consumer",
        f"hub: {settings.eventhub_name}   ·   group: {group}",
    )

    # Anchor the read position to now, so we pick up everything produced from
    # this moment on — no attach-time race like "@latest" has.
    start_time = datetime.now(timezone.utc)

    # Sign in with your local Azure identity (az login). No keys in code —
    # the namespace has local (SAS) auth disabled, so Azure AD is required.
    client = EventHubConsumerClient(
        fully_qualified_namespace=settings.eventhub_namespace,
        consumer_group=group,
        eventhub_name=settings.eventhub_name,
        credential=DefaultAzureCredential(exclude_managed_identity_credential=True),
    )

    timer_started = threading.Event()

    def on_event(partition_context, event) -> None:
        if event is None:
            return
        reading = json.loads(event.body_as_str())
        log.received(
            f"p{partition_context.partition_id}  seq={event.sequence_number:<5} "
            f"{reading['device_id']:<8} {reading['metric']}={reading['value']}"
        )

    def on_partition_initialize(partition_context) -> None:
        # The countdown (if any) starts only once the reader is actually
        # attached, not while we were still authenticating and connecting.
        if seconds is not None and not timer_started.is_set():
            timer_started.set()
            t = threading.Timer(seconds, client.close)
            t.daemon = True
            t.start()
        log.done(f"p{partition_context.partition_id}  reader attached — now run the producer")

    if seconds is None:
        log.info("attaching… then reading until you press Ctrl-C")
    else:
        log.info(f"attaching… then reading for {seconds}s (Ctrl-C to stop early)")
    print()

    try:
        with client:
            client.receive(
                on_event=on_event,
                on_partition_initialize=on_partition_initialize,
                starting_position=start_time,
                max_wait_time=5,
            )
    except KeyboardInterrupt:
        log.warn("interrupted")

    print()
    log.info("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--group", default=None, help="consumer group (default from .env)")
    parser.add_argument(
        "--seconds",
        type=int,
        default=None,
        help="stop this many seconds after attaching (default: run until Ctrl-C)",
    )
    args = parser.parse_args()
    main(args.group or settings.consumer_group, args.seconds)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--group", default=None, help="consumer group (default from .env)")
    parser.add_argument("--seconds", type=int, default=20)
    args = parser.parse_args()
    main(args.group or settings.consumer_group, args.seconds)
