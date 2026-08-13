---
title: Aborting tasks
description: Cancel a task with job.abort(), whether it is waiting in the queue or already running on a worker.
---

`await job.abort()` cancels a task wherever it currently is — waiting on a delay, queued
for pickup, or already running on some worker.

```python
job = await slow_report.options(delay_ms=60_000).enqueue()

if await job.abort():                # False if it already finished
    result = await job.result(timeout=5)
    print(result.aborted)            # True
    print(result.success)            # False
```

It returns `False` when there is nothing left to cancel: the task already finished, or the
id is unknown. Aborting twice is safe — the second call just returns `False`.

## What an aborted task looks like

An aborted task ends as an ordinary **failed** [`TaskResult`](/reference/api/#taskresult)
with the `aborted` flag set, so it never retries and `result(timeout=)` returns as soon as
it settles.

```python
result = await job.result(timeout=5)
if result.aborted:
    print("cancelled")
elif not result.success:
    print("failed:", result.value)
```

`aborted` is a property — `not success and value == "aborted"` — so an abort is easy to tell
apart from a task that failed on its own.

## Where the task was when you aborted it

| Where the task is | What abort does |
|---|---|
| Waiting on a delay or schedule | Dropped from the delayed set and finalized immediately. |
| Queued for pickup | The next worker to reach it skips it instead of running it. |
| Running | The worker holding it cancels it, within about a millisecond. |

The first two cases are settled by a single Lua script, so the result is ready by the time
`abort()` returns. The third goes out on a Redis pub/sub channel that every running worker
subscribes to; the one holding the task cancels it and stores the aborted result.

:::caution[Aborting a queued unique task]
A task that is queued for pickup stays in the stream until a worker reaches it —
only its consumer can drop it. It therefore still counts as in flight, so a
[unique](/guides/enqueuing/#unique-tasks) call enqueued in that gap joins the job
you just aborted instead of starting a new one, and comes back aborted with it.
Enqueue the replacement after the abort has settled — `await job.result()`
returns as soon as it has.
:::

:::caution[Burst workers can't cancel mid-flight]
The abort channel is only watched by a long-running worker. A worker started with
`--burst` drains the queue and exits, so it doesn't subscribe. Aborts are still honored
there — the worker checks for one before starting each task — just not once a task is
already running.
:::

## Sync tasks can't be interrupted

Cancellation is `asyncio` cancellation, so it reaches a task at its next `await`.

- **Async tasks** are cancelled at the next suspension point. A task that catches
  `CancelledError` and carries on will keep running.
- **Sync tasks** run in a thread (see [Defining tasks](/guides/tasks/)), and Python can't
  interrupt a running thread. The worker stops waiting and reports the task aborted, but
  the function itself runs to completion.

If you need a sync task to stop early, have it check a flag of your own between chunks of
work.

## App-level access

With a task id but no `Job` handle:

```python
await app.abort(task_id)
```
