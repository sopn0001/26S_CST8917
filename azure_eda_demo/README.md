# Event-Driven Architecture on Azure — demo repo

Three runnable demos covering Azure Storage Queues, Service Bus and Event Hubs.

The scenario is a campus food-ordering system. It uses three services at once,
because each flow has different requirements:

| Flow | Requirement | Service |
|---|---|---|
| Place an order | Must not be lost. Locked while processed. | Service Bus queue |
| Fryer telemetry | Very high volume, append-only log | Event Hubs |
| Generate receipts | Cheap background work | Storage Queue |

---

## Setup

**1. Provision the Azure resources** (do this before the session — it takes
about four minutes)

```bash
az login
bash infra/provision.sh
```

This creates a resource group, a storage account with a receipt queue, a
Service Bus namespace with an `orders` queue, and an Event Hubs namespace with
a 4-partition `telemetry` hub. It disables SAS auth on the namespaces, assigns
your signed-in user the data roles it needs, and writes a ready-to-use `.env`.

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

Each demo is standalone and prints what it is doing.

### 1. Storage Queues — the visibility timeout & lease renewal

```bash
python 01_storage_queue/producer.py --count 5
python 01_storage_queue/consumer.py
```

The producer is its own process — it just puts orders on the queue, exactly as a
separate service would in production.

The consumer walks one message through its full lifecycle:

1. **peek** — look at the next message without dequeuing it.
2. **receive** — dequeue it with a 10-second visibility timeout; the message is
   now hidden from other consumers, and you hold a pop receipt.
3. **process** — pretend to work for 9 seconds, nearly the whole window.
4. **update** — renew the lease for another 10 seconds *before* it expires, so a
   competing consumer never sees the message and it isn't processed twice.
5. **delete** — the only call that actually removes the message.

**The point:** receiving does not remove a message, it hides it. If your work
outlasts the visibility timeout, the message reappears and someone else picks it
up — that is at-least-once delivery, and why every handler must be idempotent.
`update_message` with a fresh timeout is how a long-running handler keeps its
lease. Storage Queues have no dead-letter queue; you enforce any poison
threshold yourself by watching `dequeue_count`.

### 2. Service Bus — peek-lock & lock renewal

```bash
python 02_service_bus/send_orders.py --count 5
python 02_service_bus/receive_peek_lock.py
```

The sender just puts orders on the queue. The receiver walks one message
through peek-lock:

1. **peek** — look at the next message without locking it.
2. **receive** — lock it (peek-lock); it is hidden from other consumers.
3. **process** — pretend to work for 9 seconds.
4. **renew** — `renew_message_lock` before the lock expires, so the message
   never reappears for a competing consumer.
5. **complete** — the call that removes the message.

**The point:** this is the same lifecycle as the Storage Queue demo, but the
broker manages the lock instead of a visibility timeout. If you never complete a
message, the lock expires and Service Bus redelivers it — at-least-once again.

### 3. Event Hubs — a partitioned, append-only log

This is a live tail. Start the consumer first and wait for **"reader
attached"** (authenticating and connecting takes a few seconds), then run the
producer in another terminal and watch the events arrive. Ctrl-C to stop.

```bash
# Terminal 1
python 03_event_hubs/telemetry_consumer.py

# Terminal 2 (after "reader attached")
python 03_event_hubs/telemetry_producer.py --count 10
```

**The point:** Event Hubs is not a queue. Sending appends to the end of a
partition; reading does not remove anything. Any number of consumer groups can
read the same stream at their own pace with independent bookmarks.

---

## The things worth arguing about

**1. Message or event?** A message is sent with intent to one logical handler
("charge this card"). An event is a broadcast fact ("payment was taken"). Get
this right and the service choice is nearly mechanical.

**2. At-least-once is what you actually get.** Not at-most-once, not
exactly-once. Both queue consumers here can see a redelivery, so the handler
must be idempotent. In production that guard is a unique constraint in the same
transaction as the business change — not an in-memory set.

**3. Ordering is opt-in and it costs you.** Event Hubs partition keys give
ordering *per key*. Global ordering means one partition and one consumer, which
means you have thrown away the architecture. Ask whether the requirement really
means "per customer" — it usually does.

---

## Common problems

**`Missing environment variable: ...`** — `.env` was not found or is empty.
Run `infra/provision.sh`, or copy `.env.example` to `.env` and fill it in.

**`Unauthorized` / `amqp:unauthorized-access` / `AuthorizationFailure`** — your
signed-in identity is missing a data role, or the role was just assigned and is
still propagating (allow a few minutes). The namespaces have SAS auth disabled,
so a connection string will not work — you must be signed in with `az login`.

**Event Hubs consumer reads nothing** — start the consumer *first* and wait for
"reader attached" before running the producer. Its read position is anchored to
the moment it started, so events produced before it launched are not shown.

---

## Layout

```
shared/            config, domain models, console logging, idempotency store
01_storage_queue/  producer + consumer, visibility timeout, lease renewal
02_service_bus/    send + peek-lock receive, lock renewal
03_event_hubs/     producer + live-tail consumer
infra/             provision + cleanup (bash and PowerShell)
```

## A note on credentials

Every script uses `DefaultAzureCredential`, the passwordless approach
recommended by the Azure quickstarts. Sign in once with `az login` and the
signed-in identity is used automatically — no keys in code or `.env`:

```python
from azure.identity import DefaultAzureCredential

client = QueueClient(
    account_url="https://<account>.queue.core.windows.net",
    queue_name="receipts",
    credential=DefaultAzureCredential(exclude_managed_identity_credential=True),
)
```

`provision.sh` assigns the signed-in user **Storage Queue Data Contributor** on
the storage account, **Azure Service Bus Data Owner** on the Service Bus
namespace, and **Azure Event Hubs Data Owner** on the Event Hubs namespace.
Newly assigned roles can take a few minutes to propagate. On an Azure VM,
`exclude_managed_identity_credential=True` keeps auth on your `az login` user
instead of the VM's managed identity. `.env` is gitignored — keep it that way.
