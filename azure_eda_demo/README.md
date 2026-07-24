# Event-Driven Architecture on Azure — demo repo

Companion code for the two-hour workshop. Five runnable demos covering Azure
Storage Queues, Service Bus, Event Hubs and Event Grid, plus the same workload
rewritten as Azure Functions.

The scenario is a campus food-ordering system. It deliberately uses all four
services at once, because each flow has different requirements:

| Flow | Requirement | Service |
|---|---|---|
| Place an order | Must not be lost. Retries, DLQ. | Service Bus queue |
| Broadcast order status | Two teams need their own copy | Service Bus topic |
| Fryer telemetry | Very high volume, replayable | Event Hubs |
| Generate receipts | Cheap background work | Storage Queue |
| Menu file uploaded | Discrete reaction, push | Event Grid |

---

## Setup

**1. Provision the Azure resources** (do this before the session — it takes
about four minutes)

```bash
az login
bash infra/provision.sh
```

This creates a resource group, storage account, Service Bus namespace with a
queue/topic/two subscriptions, an Event Hubs namespace with 4 partitions and
three consumer groups, and an Event Grid custom topic — then writes a filled-in
`.env` for you.

**2. Install the SDKs**

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

**3. Check it works**

```bash
python 01_storage_queue/producer.py --count 3
```

**When you are finished** — these resources bill by the hour:

```bash
RG=rg-eda-demo bash infra/cleanup.sh
```

---

## Run order

Each demo is standalone and prints what it is doing. Roughly five minutes each.

### 1. Storage Queues — the visibility timeout

```bash
python 01_storage_queue/producer.py --count 5
python 01_storage_queue/consumer.py --slow
```

**Break it on purpose:** hit `Ctrl-C` while a message is in flight, wait for the
timeout, run the consumer again. The message is back with `dequeue_count`
incremented. That is at-least-once delivery, and it is why every handler in
this repo is idempotent.

Also watch the `poutine` orders — they fail deliberately, so you can see manual
poison-message handling. Storage Queues have no dead-letter queue; you enforce
the threshold yourself.

### 2. Service Bus — peek-lock, retries, dead lettering

```bash
python 02_service_bus/send_orders.py --count 8
python 02_service_bus/receive_peek_lock.py
python 02_service_bus/inspect_dlq.py
python 02_service_bus/inspect_dlq.py --replay
```

The `ramen` orders fail every time. Watch `delivery_count` climb, then watch
them land in the DLQ with a reason and description attached.

Competing consumers — add workers, and the same queue distributes across them:

```bash
python 02_service_bus/send_orders.py --count 30
python 02_service_bus/receive_peek_lock.py --workers 3
```

### 3. Service Bus topics — fan-out with broker-side filters

Three terminals:

```bash
python 02_service_bus/topic_fanout.py --listen notify    # terminal 1
python 02_service_bus/topic_fanout.py --listen audit     # terminal 2
python 02_service_bus/topic_fanout.py --publish          # terminal 3
```

`notify` receives everything. `audit` has a SQL filter (`amount > 13`) and only
ever sees the large orders. The filter runs inside the broker — the small
orders are never delivered to `audit` at all.

### 4. Event Hubs — partitions, consumer groups, replay

```bash
python 03_event_hubs/telemetry_producer.py --count 5000
python 03_event_hubs/telemetry_consumer.py
```

Then the thing a queue cannot do:

```bash
python 03_event_hubs/telemetry_consumer.py --from-start
python 03_event_hubs/telemetry_consumer.py --group archive --from-start
```

Reading does not remove anything. Three consumer groups can read the same
stream at three different speeds with independent bookmarks.

Compare ordering behaviour with and without a partition key:

```bash
python 03_event_hubs/telemetry_producer.py --count 2000 --no-key
```

### 5. Event Grid — push delivery

```bash
python 04_event_grid/publish_events.py
python 04_event_grid/publish_events.py --schema cloudevent
```

There is no consumer to run — Event Grid pushes to whatever subscribed. Wire up
a subscription to the Functions app below, then break the handler and watch the
retries and the dead-letter blob.

### 6. The same thing as Azure Functions

```bash
cd 05_functions
cp local.settings.json.example local.settings.json   # fill in connections
func start
```

All four trigger types in one `function_app.py` (Python v2 model). The host
handles the receive loop, lock renewal and checkpointing. What you still own is
idempotency and deciding what a failure means — which is the point.

---

## The five things worth arguing about

**1. Message or event?** A message is sent with intent to one logical handler
("charge this card"). An event is a broadcast fact ("payment was taken"). Get
this right and the service choice is nearly mechanical.

**2. At-least-once is what you actually get.** Not at-most-once, not
exactly-once. Every consumer here has an idempotency guard, and every one of
them needs it. In production that guard is a unique constraint in the same
transaction as the business change — not an in-memory set.

**3. Ordering is opt-in and it costs you.** Service Bus sessions and Event Hubs
partition keys both give ordering *per key*. Global ordering means one partition
and one consumer, which means you have thrown away the architecture. Ask whether
the requirement really means "per customer" — it usually does.

**4. The dead-letter queue needs an owner.** A DLQ with no alert is silent data
loss. Alert on `DeadletteredMessages > 0`, log the reason and description, and
build the replay tool before 2am rather than during it.

**5. Design the failure path first.** Retries, poison thresholds, compensating
actions and dead letters are the architecture. The happy path is the easy part.

---

## Common problems

**`Missing environment variable: ...`** — `.env` was not found or is empty.
Run `infra/provision.sh`, or copy `.env.example` to `.env` and fill it in.

**`ServiceBusAuthorizationError`** — the connection string is namespace-level
but the queue or topic does not exist. Re-run the provisioning script.

**Event Hubs consumer reads nothing** — you are at `@latest` and nothing new is
arriving. Run the producer in another terminal, or pass `--from-start`.

**Event Hubs consumer reads nothing the second time** — working as intended.
Your checkpoint is at the end of the stream. Pass `--from-start` to rewind.

**Scaling the consumer changes nothing** — you have more instances than
partitions. One active reader per partition per consumer group is the ceiling.
The hub here has 4.

---

## Layout

```
shared/            config, domain models, console logging, idempotency store
01_storage_queue/  producer + consumer, visibility timeout, poison handling
02_service_bus/    send, peek-lock receive, DLQ inspect/replay, topic fan-out
03_event_hubs/     partitioned producer, checkpointing consumer, replay
04_event_grid/     custom topic publisher (EventGrid + CloudEvents schemas)
05_functions/      all four triggers, Python v2 programming model
infra/             provision.sh, cleanup.sh
```

## A note on credentials

These scripts use connection strings because that keeps the classroom setup to
one command. Production uses managed identity:

```python
from azure.identity import DefaultAzureCredential

client = ServiceBusClient(
    fully_qualified_namespace="sb-yourns.servicebus.windows.net",
    credential=DefaultAzureCredential(),
)
```

Every SDK in this repo accepts a `credential=` in place of a connection string.
`.env` and `local.settings.json` are gitignored — keep it that way.
