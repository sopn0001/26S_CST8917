"""Domain payloads for the campus food-ordering demo.

Two shapes on purpose — they are not the same thing and they do not travel
on the same service:

    Order      a COMMAND. "Make this food." Must not be lost. -> Service Bus
    Telemetry  an EVENT.  "This happened."  One loss is noise. -> Event Hubs
"""
import json
import random
import uuid
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone

MENU = [
    ("burrito", 12.50),
    ("ramen", 14.00),
    ("poutine", 9.75),
    ("bibimbap", 15.25),
    ("shawarma", 11.00),
]

STATIONS = ["grill", "wok", "cold-line"]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class Order:
    """A command. Imperative: 'prepare this order'."""

    order_id: str
    student_id: str
    item: str
    amount: float
    station: str
    placed_at: str = field(default_factory=_now)

    @staticmethod
    def random(student_id: str | None = None) -> "Order":
        item, price = random.choice(MENU)
        return Order(
            order_id=f"order-{uuid.uuid4().hex[:8]}",
            student_id=student_id or f"student-{random.randint(1000, 1099)}",
            item=item,
            amount=price,
            station=random.choice(STATIONS),
        )

    def to_json(self) -> str:
        return json.dumps(asdict(self))


@dataclass
class OrderStatusChanged:
    """An event. Past tense: this already happened."""

    order_id: str
    status: str          # accepted | cooking | ready | collected
    station: str
    amount: float
    occurred_at: str = field(default_factory=_now)

    def to_json(self) -> str:
        return json.dumps(asdict(self))


@dataclass
class Telemetry:
    """A high-volume event. Losing one is a rounding error."""

    device_id: str       # used as the Event Hubs partition key
    metric: str
    value: float
    recorded_at: str = field(default_factory=_now)

    @staticmethod
    def random(device_id: str | None = None) -> "Telemetry":
        return Telemetry(
            device_id=device_id or f"fryer-{random.randint(1, 4)}",
            metric=random.choice(["oil_temp_c", "queue_length", "wait_seconds"]),
            value=round(random.uniform(20, 195), 2),
        )

    def to_json(self) -> str:
        return json.dumps(asdict(self))
