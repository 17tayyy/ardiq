---
title: Enqueuing & scheduling
description: Dispatch tasks with .enqueue, and override priority, delays, scheduled runs, task ids and expiry with .options.
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
