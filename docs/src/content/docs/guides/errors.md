---
title: Handling failures
description: Steer a task's retries with the Retry exception, and report every failed attempt to Sentry or your own reporter with @app.on_error.
---

A task that raises is retried up to `max_retries` times and then stored as a failed
[`TaskResult`](/reference/api/#taskresult) — see [Retries](/guides/tasks/#retries) for the
per-task settings. This page covers the two ways to take part in that: deciding a retry
from inside the task, and being told about every attempt that goes wrong.

## Retrying on demand

`Retry` asks for another attempt with a delay the task picks, rather than the backoff it
was configured with. The obvious case is a rate limit that tells you how long to wait:

```python
from ardiq import Retry


@app.task(max_retries=5)
async def call_api(user_id: int) -> dict:
    response = await client.get(URL, params={"user": user_id})
    if response.status_code == 429:
        raise Retry("rate limited", delay_ms=30_000)
    return response.json()
```

`Retry(message, *, delay_ms=None)`. Without `delay_ms` it falls back to the task's usual
backoff, so `raise Retry()` just means "run me again".

It still counts against `max_retries` — a task cannot loop forever by raising it. When the
budget runs out, the task fails with the `Retry` as its error, exactly like any other
exception:

```python
result = await job.result()
print(result.success)   # False
print(result.value)     # Retry('rate limited')
```

:::note[Burst workers drop pending retries]
A retry goes back through the delayed queue, and `--burst` exits once the queue drains
instead of waiting for work that isn't due yet. Retries need a long-running worker.
:::

## Reporting errors

`@app.on_error` registers a hook that runs on every failed attempt, before ArdiQ decides
between retrying and failing. This is the hook a reporter like Sentry goes in:

```python
import sentry_sdk


@app.on_error
def report(ctx):
    sentry_sdk.capture_exception(ctx.exc)
    log.warning("%s failed on try %s (retrying: %s)", ctx.name, ctx.tries, ctx.will_retry)
```

The hook is handed an [`ErrorContext`](/reference/api/#errorcontext):

| Field | Type | Description |
|---|---|---|
| `name` | `str` | The task's registered name. |
| `task_id` | `str` | The job id, the same one `Job.id` carries. |
| `exc` | `BaseException` | The exception the attempt raised. |
| `tries` | `int` | The attempt that just failed, counting from 1. |
| `will_retry` | `bool` | Whether another attempt is coming. |

Hooks may be sync or async, and you can register as many as you like — all of them run, in
registration order. One that raises is logged and never changes the task's outcome.

```python
@app.on_error
async def to_dead_letter(ctx):
    if not ctx.will_retry:
        await archive.insert(ctx.task_id, ctx.name, repr(ctx.exc))
```

### When it fires

| Situation | Fires? |
|---|---|
| The task raises | Yes, on every attempt |
| The attempt hits its `timeout` | Yes, with a `TimeoutError` |
| The worker doesn't know the task | Yes, with a `LookupError` |
| The task raised `Retry` and will run again | No |
| That `Retry` ran out of attempts | Yes, with `will_retry=False` |
| The task was [aborted](/guides/aborting/) | No |

A retry the task asked for is control flow, not a fault, so it stays out of the hooks
until it gives up. An abort is something you asked for too, and never reaches them.

:::caution[Hooks run on the worker's event loop]
A slow hook holds up the worker like a slow task would, and a sync one blocks the loop
outright. Keep them to a handoff — an SDK call that buffers, a queue push — and do the
heavy work elsewhere.
:::
