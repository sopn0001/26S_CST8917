"""DEMO 4 — Event Grid: publishing discrete facts to a router.

Event Grid is PUSH. There is no consumer to run and nothing to poll — the
service delivers to whoever subscribed, retries with exponential backoff for
up to 24 hours, and dead-letters to a storage container after that.

Note how small these payloads are. An Event Grid event is a notification:
"this happened, here is the id". It is not a data pipe. If the handler needs
the full object, it fetches it — that is the claim-check pattern.

Two schemas are shown below:

    EventGridEvent  the original Azure schema
    CloudEvent      the CNCF CloudEvents 1.0 standard — prefer this for new
                    work, it is portable across clouds and brokers

Run:
    python 04_event_grid/publish_events.py
    python 04_event_grid/publish_events.py --schema cloudevent --count 5
"""
import argparse
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from azure.core.credentials import AzureKeyCredential
from azure.core.messaging import CloudEvent
from azure.eventgrid import EventGridEvent, EventGridPublisherClient

from shared import log
from shared.config import settings

MENU_FILES = [
    "menus/2026-fall/week-01.csv",
    "menus/2026-fall/week-02.csv",
    "menus/2026-fall/allergens.json",
]


def build_eventgrid_events(count: int) -> list[EventGridEvent]:
    events = []
    for i in range(count):
        path = MENU_FILES[i % len(MENU_FILES)]
        events.append(
            EventGridEvent(
                # subject is what prefix/suffix filters match on — design it
                # as a path, most specific segment last.
                subject=f"/campus/dining/{path}",
                event_type="Campus.Dining.MenuFileUploaded",
                data={
                    "blobUrl": f"https://contoso.blob.core.windows.net/{path}",
                    "sizeBytes": 4096 + i,
                    "uploadedBy": "dining-services",
                },
                data_version="1.0",
            )
        )
    return events


def build_cloud_events(count: int) -> list[CloudEvent]:
    events = []
    for i in range(count):
        path = MENU_FILES[i % len(MENU_FILES)]
        events.append(
            CloudEvent(
                source="/campus/dining/uploader",
                type="Campus.Dining.MenuFileUploaded",
                subject=f"/campus/dining/{path}",
                id=str(uuid.uuid4()),
                data={
                    "blobUrl": f"https://contoso.blob.core.windows.net/{path}",
                    "sizeBytes": 4096 + i,
                },
            )
        )
    return events


def main(schema: str, count: int) -> None:
    log.banner(
        "Event Grid — custom topic publisher",
        f"schema: {schema}   ·   {count} events",
    )

    client = EventGridPublisherClient(
        settings.eventgrid_endpoint,
        AzureKeyCredential(settings.eventgrid_key),
    )

    events = (
        build_cloud_events(count) if schema == "cloudevent" else build_eventgrid_events(count)
    )

    # One call, many events. Event Grid fans them out to every matching
    # subscription — you do not know or care how many there are.
    client.send(events)

    for event in events:
        log.sent(f"{event.subject}")

    print()
    log.info("Delivery is push. Check your handler's logs, not a queue.")
    log.info("If the handler returns non-2xx, Event Grid retries for up to 24 h,")
    log.info("then writes the event to the dead-letter blob container.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", choices=["eventgrid", "cloudevent"], default="eventgrid")
    parser.add_argument("--count", type=int, default=3)
    args = parser.parse_args()
    main(args.schema, args.count)
