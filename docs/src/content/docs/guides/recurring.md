---
title: Recurring tasks
description: Run tasks on a schedule with @app.cron — standard cron expressions (UTC) or fixed every= intervals.
---

Register a task to run repeatedly with `@app.cron`. Give it either a standard 5-field
**cron expression** (evaluated in UTC) or a fixed **`every=` interval**:

```python
from datetime import timedelta
from ardiq import Ardiq

app = Ardiq(queue_name="jobs")


@app.cron("0 3 * * *")                   # every day at 03:00 UTC
async def nightly_report():
    ...


@app.cron(every=30)                      # every 30 seconds
async def heartbeat():
    ...


@app.cron(every=timedelta(minutes=5))    # every 5 minutes
async def poll_inbox():
    ...
```

`@app.cron` registers an ordinary task (it shows up in `app.tasks` and can still be
enqueued by hand) plus a schedule. Recurring tasks **fire while a worker is running**.

## Cron expressions

The cron string has the standard five fields — **minute hour day-of-month month
day-of-week** — evaluated in **UTC**:

```
┌───────────── minute        (0–59)
│ ┌─────────── hour          (0–23)
│ │ ┌───────── day of month  (1–31)
│ │ │ ┌─────── month         (1–12)
│ │ │ │ ┌───── day of week   (0–6, Sunday = 0; 7 also = Sunday)
│ │ │ │ │
* * * * *
```

Each field supports `*`, single values, lists (`1,15`), ranges (`9-17`), and steps
(`*/5`, `0-30/10`). As in classic cron, when **both** day-of-month and day-of-week are
restricted, a task fires when **either** matches.

```python
@app.cron("*/15 9-17 * * 1-5")     # every 15 min, 09:00–17:00 UTC, Mon–Fri
async def business_hours_sync():
    ...
```

:::note[Supported subset]
ArdiQ ships a small, dependency-free cron parser covering the common subset above. It
does **not** support `L`, `#`, `@yearly`-style nicknames, named months/days, or seconds.
For sub-minute schedules, use `every=`.
:::

## Intervals

`every=` takes a number of seconds (`int` or `float`) or a `timedelta`. Occurrences are
aligned to the epoch, so `every=30` fires at `:00` and `:30` of each minute. Use it for
the sub-minute cadences cron can't express:

```python
@app.cron(every=0.5)     # twice a second
async def sample():
    ...
```

## Options

`@app.cron` accepts the same per-task options as [`@app.task`](/guides/tasks/) —
`name`, `max_retries`, `backoff_ms`, `timeout`, `priority` — applied to every occurrence:

```python
@app.cron("0 * * * *", priority="low", timeout=60)
async def hourly_cleanup():
    ...
```

## How it works

Each due occurrence is enqueued as an ordinary job with a deterministic id
(`cron:<name>:<fire-ms>`), reusing the same delayed queue as `delay_ms` / `schedule_ms`.
That means:

- every occurrence has its own **result, status, retries and timeout**, like any task;
- the **same occurrence is never enqueued twice**, even with several workers running
  (Redis `SET NX` dedup);
- a worker that was down simply **skips missed periods** — it always schedules the next
  occurrence after *now*, never a backlog.

A worker re-checks each cron's next fire time every `cron_poll_s` (default 1s, see
[Configuration](/reference/configuration/)). Recurring tasks don't run under
[burst mode](/guides/worker/#burst-mode), which exits as soon as the queue drains.
