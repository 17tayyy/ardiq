---
title: Metrics
description: Outcomes and run and wait times per task and lane, counted by the Rust core without instrumenting a single task.
---

The core times every attempt it hands to Python, so a worker knows how each task is doing
without a line of instrumentation in the task itself. `app.metrics()` reads those numbers,
one [`TaskMetrics`](/reference/api/#taskmetrics) per task and lane:

```python
for m in app.metrics():
    print(m.fn_name, m.priority, m.succeeded, m.failed, m.wait.sum / m.wait.count)
```

It reads memory, not Redis, so it is a plain call and cheap enough to make as often as you
like.

## What is counted

Every attempt lands in exactly one outcome:

| Field | Attempts that |
|---|---|
| `succeeded` | returned. |
| `retried` | failed with retries left, or raised `Retry`. |
| `failed` | failed for good, unknown task names included. Each one is in the [dead letter queue](/guides/dead-letters/). |
| `aborted` | were [aborted](/guides/aborting/), before or while running. |

And two histograms, in seconds:

- **`run`** is how long each attempt took, retries included.
- **`wait`** is how long the task sat ready before a worker picked it up. A delayed task or
  a retry is ready from its due time, so `delay_ms=60_000` does not show up as a minute of
  waiting. A `wait` that keeps climbing means the workers are falling behind.

`Histogram.buckets` pairs each upper bound with how many observations fell at or under it,
the cumulative shape Prometheus uses. The bounds are 0.005, 0.01, 0.025, 0.05, 0.1, 0.25,
0.5, 1, 2.5, 5, 10, 30, 60 and 300 seconds, and `count` also includes anything above the
last one.

## Whose numbers they are

The counts belong to the process that ran the tasks. They start at zero when it starts, so
a producer that never runs a worker reads an empty list, and with `ardiq run --workers N`
each worker process keeps its own. To ship them somewhere, read them inside the worker,
for example from the [lifespan](/guides/lifespan/):

```python
import asyncio


@app.lifespan
async def lifespan():
    async def report():
        while True:
            await asyncio.sleep(60)
            for m in app.metrics():
                statsd.gauge(f"ardiq.{m.fn_name}.failed", m.failed)

    reporter = asyncio.create_task(report())
    yield
    reporter.cancel()
```

`wait` compares the worker's clock with the one that set the task's ready time, usually the
enqueuer's. Keep your hosts on NTP: a worker whose clock runs behind reports less waiting
than there was, never a negative amount.
