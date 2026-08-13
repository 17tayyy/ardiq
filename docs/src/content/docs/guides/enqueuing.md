---
title: Enqueuing & scheduling
description: Dispatch tasks with .enqueue or by name with app.send, and override priority, delays, scheduled runs, task ids and expiry with .options.
---

Calling `await task.enqueue(*args, **kwargs)` serializes the arguments, pushes the job onto
Redis, and returns a [`Job`](/reference/api/#job) handle you can use to read status and
results.

```python
job = await add.enqueue(2, 3)
print(job.id)   # a uuid hex, unless you set one yourself
```

Enqueuing is async because the round-trip to Redis is async — call it from within an event
loop.

## Enqueuing by name

The side that enqueues doesn't have to be the side that runs. `app.send(name, *args,
**kwargs)` dispatches by name, so a web service can put work on the queue without
importing the task module — or the dependencies it pulls in:

```python
from ardiq import Ardiq
from fastapi import FastAPI

api = FastAPI()
queue = Ardiq(redis_url="redis://localhost:6379", queue_name="example")


@api.post("/reports")
async def create_report(user_id: int):
    job = await queue.send("build_report", user_id, format="pdf")
    return {"job_id": job.id}
```

Nothing is checked locally: the name is resolved by the worker that picks the task up,
and one it doesn't know [fails there](/guides/results/) like any other error.

Because `send` forwards every keyword to the task, it takes no options of its own. For
those, `app.ref(name)` returns the same handle `@app.task` gives you:

```python
await queue.ref("build_report").options(delay_ms=60_000, priority="low").enqueue(7)
await queue.ref("build_report", priority="low").enqueue(7)   # a default lane for the handle
```

A `ref` can be enqueued but not called — there is no local function behind it.

:::caution[Priority does not travel with the name]
Everything else on `@app.task(...)` — `max_retries`, `backoff_ms`, `timeout` — is applied
by the worker, which has the registry and can look it up. Priority is the exception: it
picks which stream the task goes into, so it is settled by the producer, before the
payload leaves.

A task declared `@app.task(priority="high")` and dispatched by name lands in the app's
[`default_priority`](/guides/tasks/#priorities) instead, with no warning. Pass it at the
call site:

```python
await queue.ref("build_report", priority="high").enqueue(7)
```

The fallback being the middle lane makes forgetting it survivable rather than disastrous,
but the work still won't be in the lane you declared for it.
:::

## Enqueuing in bulk

One `.enqueue()` is one round trip to Redis. Fanning out over a list, that adds
up — 20,000 of them take about 2.7s. `enqueue_many` sends the whole batch
instead, and the same 20,000 take **0.17s**.

Build each call with `.prepare(...)` — same arguments `.enqueue` takes, checked
against the task's signature the same way — and hand the lot to the app:

```python
jobs = await queue.enqueue_many(charge.prepare(oid) for oid in order_ids)
```

It takes any iterable, so a generator is fine for large fan-outs. You get back
one `Job` per item, in the order you gave them.

A batch can mix tasks and per-call options freely — that is the point of
separating `prepare` from `enqueue`:

```python
await queue.enqueue_many([
    charge.prepare(42),
    send_receipt.prepare("customer@example.com"),
    reindex.options(priority="high").prepare(42),
])
```

Priorities are validated across the whole batch *before* anything is sent, so a
lane nobody reads raises and enqueues nothing, rather than leaving half the
batch in Redis.

## Unique tasks

Some work must not queue up twice. Two "rebuild this shop's index" jobs for the
same shop do the same thing: the second is wasted at best, and a race at worst.
Declare the task `unique=True` and an identical call that is already waiting or
running is not enqueued a second time:

```python
@app.task(unique=True)
async def rebuild_index(shop_id: int): ...


first = await rebuild_index.enqueue(42)
second = await rebuild_index.enqueue(42)   # nothing new is enqueued

assert second.id == first.id               # the job already in flight
```

You still get a `Job` back — the one already doing the work — so the caller
never has to treat "someone got there first" as an error.

Identity is the call itself: the task's name plus its arguments. So
`rebuild_index(42)` and `rebuild_index(43)` are two different jobs, and keyword
order is irrelevant (`notify(to="ada", subject="hi")` is the same call as
`notify(subject="hi", to="ada")`). The id you get back shows it:
`unique:rebuild_index:9f2c…`, which is also what you'll see in Redis.

The window is exactly as long as the task exists — from enqueue until it
finishes, retries included. Once it has finished, the same call can be enqueued
again, and that new run **replaces the previous result** under the same id: an
older `Job` handle you kept around will read the new run's outcome, not the one
it saw before.

Because the id is derived from the payload, every process computes the same one.
Duplicates collapse inside a batch, and a producer with no registry can dedup
too:

```python
# one job, not three
await queue.enqueue_many([rebuild_index.prepare(42)] * 3)

# from a web service that never imports the task
await queue.ref("rebuild_index", unique=True).enqueue(42)
```

`unique` is a per-call option like any other, so `.options(unique=True)` turns it
on for one dispatch and `.options(unique=False)` turns it off for a task that
declared it. Passing your own `task_id` wins over both — an explicit id is
already a deduplication key.

:::note[Not a lock on the function]
Uniqueness is per *call*, not per task: a hundred `rebuild_index` jobs for a
hundred different shops all run, as they should. There is nothing here that
limits how many instances of one task run at once — that's what
[`concurrency`](/reference/configuration/) is for.
:::

## Per-call options

For one-off overrides, chain `.options(...)` before `.enqueue(...)`:

```python
await add.options(priority="high", delay_ms=5000).enqueue(2, 3)
```

`.options(...)` accepts:

| Option        | Type          | Default | Description |
|---------------|---------------|---------|-------------|
| `task_id`     | `str \| None` | a uuid  | Set your own job id — also used for deduplication. |
| `priority`    | `str \| None` | task's default | Override the priority lane for this call. |
| `delay_ms`    | `int`         | `0`     | Wait this many ms from **now** before the task becomes runnable. |
| `schedule_ms` | `int`         | `0`     | Run at this absolute epoch-ms timestamp. |
| `expire_ms`   | `int`         | `0`     | Drop the job if it hasn't started within this window. |
| `unique`      | `bool \| None`| task's default | Dedup this call against an identical one in flight; see [Unique tasks](#unique-tasks). |

### Delayed tasks

Run something after a relative delay:

```python
# fire in 30 seconds
await reminder.options(delay_ms=30_000).enqueue(user_id)
```

### Scheduled tasks

Run something at a specific wall-clock time, using an absolute timestamp in epoch ms:

```python
import time

run_at = int(time.time() * 1000) + 3_600_000   # one hour from now
await digest.options(schedule_ms=run_at).enqueue()
```

While a job is waiting in the delayed/scheduled set, its [status](/guides/results/) is
`scheduled`.

### Custom ids & deduplication

Setting `task_id` lets you control the job id — useful to make an enqueue idempotent: the
same id won't create a duplicate job.

```python
await sync_account.options(task_id=f"sync:{account_id}").enqueue(account_id)
```

### Expiry

`expire_ms` drops a job that has been waiting too long to start — useful for work that's
worthless if it's stale:

```python
# if no worker picks it up within 60s, forget it
await notify.options(expire_ms=60_000).enqueue(user_id)
```

## Priorities

Higher-priority lanes are drained first. Define the lanes (lowest-first) on the app, then
target one per task or per call:

```python
app = Ardiq(priorities=["low", "default", "high"])

await report.options(priority="low").enqueue()    # batch work
await alert.options(priority="high").enqueue()     # jump the queue
```

See [Defining tasks](/guides/tasks/#priorities) for setting a task's default lane.

## Reading the result

`.enqueue(...)` returns immediately with a `Job`. To get the outcome, see
[Results & introspection](/guides/results/):

```python
job = await add.enqueue(2, 3)
result = await job.result(timeout=5)   # waits up to 5s
print(result.value)                    # 5
```
