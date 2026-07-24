"""DEMO 3b — Event Hubs: consuming, checkpointing, and replay.

CONSUMER GROUPS
---------------
Each consumer group is an independent reader with its own bookmark. Three
teams can read the same stream at three different speeds without touching
each other. Adding one is free — do not share $Default between two apps or
their checkpoints will fight.

CHECKPOINTING
-------------
Your position is stored in Blob Storage, not in the hub. On restart you
resume from the last checkpoint, NOT from the beginning.

    checkpoint too often  -> a storage round trip per event, slow and costly
    checkpoint too rarely -> you reprocess more after a crash

Batching every ~100 events or ~5 seconds is a reasonable default. Note the
consequence: everything between the last checkpoint and the crash is
reprocessed. At-least-once again, so the handler must be idempotent.

REPLAY — the thing a queue cannot do
------------------------------------
    --from-start   rewind to the oldest retained event and read it all again
    --no-checkpoint  read without recording position (leaves the bookmark alone)

TRY THIS DURING THE DEMO
------------------------
1. python 03_event_hubs/telemetry_consumer.py            # reads, checkpoints
2. Ctrl-C partway through
3. python 03_event_hubs/telemetry_consumer.py            # resumes where it stopped
4. python 03_event_hubs/telemetry_consumer.py --from-start
                                                          # the whole tape again

Run:
    python 03_event_hubs/telemetry_consumer.py
    python 03_event_hubs/telemetry_consumer.py --group archive --from-start
"""
import argparse
import json
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from azure.eventhub import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore

from shared import log
from shared.config import settings

CHECKPOINT_EVERY = 100          # events
CHECKPOINT_SECONDS = 5.0

_counts: Counter[str] = Counter()
_since_checkpoint: Counter[str] = Counter()
_last_checkpoint: dict[str, float] = {}
_started = time.time()


def on_event(partition_context, event) -> None:
    if event is None:
        return

    pid = partition_context.partition_id
    _counts[pid] += 1
    _since_checkpoint[pid] += 1

    reading = json.loads(event.body_as_str())

    # Print a sample rather than every event, or the terminal is unreadable.
    if _counts[pid] % 250 == 1:
        log.received(
            f"p{pid}  offset={event.offset:<10} seq={event.sequence_number:<7} "
            f"{reading['device_id']}  {reading['metric']}={reading['value']}"
        )

    # ---- batched checkpointing ------------------------------------------
    now = time.time()
    last = _last_checkpoint.get(pid, _started)
    if _since_checkpoint[pid] >= CHECKPOINT_EVERY or (now - last) >= CHECKPOINT_SECONDS:
        partition_context.update_checkpoint(event)
        log.done(f"p{pid}  checkpointed at seq={event.sequence_number} "
                 f"({_since_checkpoint[pid]} events)")
        _since_checkpoint[pid] = 0
        _last_checkpoint[pid] = now


def on_partition_initialize(partition_context) -> None:
    log.info(f"p{partition_context.partition_id}  reader attached")


def on_error(partition_context, error) -> None:
    pid = partition_context.partition_id if partition_context else "-"
    log.fail(f"p{pid}  {error}")


def main(group: str, from_start: bool, checkpointing: bool, seconds: int) -> None:
    log.banner(
        "Event Hubs — consumer",
        f"hub: {settings.eventhub_name}   ·   group: {group}   ·   "
        + ("REPLAY from oldest" if from_start else "resume from checkpoint")
        + ("" if checkpointing else "   ·   checkpointing OFF"),
    )

    checkpoint_store = None
    if checkpointing:
        checkpoint_store = BlobCheckpointStore.from_connection_string(
            settings.storage_conn, container_name=settings.checkpoint_container
        )
        log.info(f"checkpoints -> blob container '{settings.checkpoint_container}'")

    client = EventHubConsumerClient.from_connection_string(
        settings.eventhub_conn,
        consumer_group=group,
        eventhub_name=settings.eventhub_name,
        checkpoint_store=checkpoint_store,
    )

    # "-1" = the oldest retained event. "@latest" = only new arrivals.
    starting_position = "-1" if from_start else "@latest"
    if checkpointing and not from_start:
        # With a checkpoint store, a stored offset wins over this default.
        starting_position = "-1"

    handler = on_event if checkpointing else _no_checkpoint_handler
    log.info(f"reading for {seconds}s — Ctrl-C to stop early")
    print()

    try:
        with client:
            client.receive(
                on_event=handler,
                on_partition_initialize=on_partition_initialize,
                on_error=on_error,
                starting_position=starting_position,
                max_wait_time=float(seconds),
            )
    except KeyboardInterrupt:
        log.warn("interrupted — position is at the last checkpoint, not here")

    print()
    total = sum(_counts.values())
    log.info(f"read {total} events across {len(_counts)} partition(s)")
    for pid, n in sorted(_counts.items()):
        log.info(f"    partition {pid}: {n}")
    if not checkpointing:
        log.warn("checkpointing was off — the stored position did not move")


def _no_checkpoint_handler(partition_context, event) -> None:
    """Same as on_event but never calls update_checkpoint."""
    if event is None:
        return
    pid = partition_context.partition_id
    _counts[pid] += 1
    if _counts[pid] % 250 == 1:
        reading = json.loads(event.body_as_str())
        log.received(
            f"p{pid}  seq={event.sequence_number:<7} {reading['device_id']} "
            f"{reading['metric']}={reading['value']}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--group", default=None, help="consumer group (default from .env)")
    parser.add_argument("--from-start", action="store_true", help="replay from the oldest event")
    parser.add_argument("--no-checkpoint", action="store_true", help="read without saving position")
    parser.add_argument("--seconds", type=int, default=20)
    args = parser.parse_args()
    main(
        args.group or settings.consumer_group,
        args.from_start,
        not args.no_checkpoint,
        args.seconds,
    )
