"""DEMO 3a — Event Hubs: send a stream.

Event Hubs is NOT a queue. It is a partitioned, append-only log. Sending
appends to the end of a partition; nothing is ever removed by a reader.

Run:
    python 03_event_hubs/telemetry_producer.py
    python 03_event_hubs/telemetry_producer.py --count 50
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from azure.identity import DefaultAzureCredential
from azure.eventhub import EventData, EventHubProducerClient

from shared import log
from shared.config import settings
from shared.models import Telemetry


def main(count: int) -> None:
    log.banner(
        "Event Hubs — telemetry producer",
        f"hub: {settings.eventhub_name}   ·   sending {count} events",
    )

    # Sign in with your local Azure identity (az login). No keys in code —
    # the namespace has local (SAS) auth disabled, so Azure AD is required.
    producer = EventHubProducerClient(
        fully_qualified_namespace=settings.eventhub_namespace,
        eventhub_name=settings.eventhub_name,
        credential=DefaultAzureCredential(exclude_managed_identity_credential=True),
    )

    with producer:
        # One batch, one network round trip. Append each reading to the log.
        batch = producer.create_batch()
        for _ in range(count):
            reading = Telemetry.random()
            batch.add(EventData(reading.to_json()))
            log.sent(f"{reading.device_id:<8} {reading.metric}={reading.value}")
        producer.send_batch(batch)

    print()
    log.info("Now run:  python 03_event_hubs/telemetry_consumer.py")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=10)
    main(parser.parse_args().count)
