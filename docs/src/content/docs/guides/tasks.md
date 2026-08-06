---
title: Defining tasks
description: The @app.task decorator — names, retries, backoff, timeouts, priorities, and sync vs async tasks.
---

A task is a Python function registered with an `Ardiq` app via the `@app.task` decorator.
Registering it returns a [`Task`](/reference/api/#task) object you can `.enqueue(...)` or
call inline.

```python
from ardiq import Ardiq

app = Ardiq(queue_name="emails")


@app.task()
async def send_welcome(user_id: int) -> None:
    ...
```

## Sync vs async tasks

ArdiQ decides how to run a task at registration time, based on whether it's a coroutine
function:

- **`async def` tasks** run directly on the worker's event loop.
- **`def` (sync) tasks** are run in a thread pool (`asyncio.to_thread`), so a blocking call
  never freezes the loop or the rest of the worker's concurrency.

```python
@app.task()
async def fetch(url: str) -> str:        # runs on the loop
    ...


@app.task()
def resize_image(path: str) -> str:      # runs in a thread
    ...
```

:::tip
Write I/O-bound work as `async def` and CPU- or library-bound blocking work as plain `def`.
Either way, keep in mind the GIL: CPU-heavy work is serial per worker — scale out with more
workers.
:::

## Decorator options

`@app.task(...)` accepts:

| Option        | Type          | Default | Description |
|---------------|---------------|---------|-------------|
| `name`        | `str`         | function name | The name used on the wire and in the registry. Required if the callable has no `__name__`. |
| `max_retries` | `int`         | `3`     | How many times to retry after the first attempt fails. |
| `backoff_ms`  | `int`         | `0`     | Delay between retries in ms. `0` uses the core's default backoff. |
| `timeout`     | `float \| None` | `None` | Per-task timeout in **seconds**. A task that exceeds it fails (and may retry). |
| `priority`    | `str \| None` | `None`  | Default priority lane for this task (see [Priorities](#priorities)). |

```python
@app.task(name="email.welcome", max_retries=5, backoff_ms=2000, timeout=30)
async def send_welcome(user_id: int) -> None:
    ...
```

## Names must be unique

A task's name is its identity on the wire, and by default it is just the function's
`__name__` — not the module path. Registering the same name twice raises, naming the
module that already owns it:

```python
# whatsapp.py
@app.task()
def forward_message(...): ...

# twilio.py — importing this now raises at import time
@app.task()
def forward_message(...): ...
# ValueError: task 'forward_message' is already registered by myapp.whatsapp — ...
```

This is deliberate: a queue that let the second registration win would silently stop
running the first task, with no error and no log line, and you would find out from the
work that never happened. `@app.cron` shares the registry, so a cron and a task cannot
collide either.

Give one of them an explicit name — namespacing by module is a good habit:

```python
@app.task(name="whatsapp.forward_message")
def forward_message(...): ...
```

## Knowing which task you are

`current_task()` returns the task running right now — its id, name and attempt number —
so a task can put them in its own logs without being handed them:

```python
from ardiq import current_task


@app.task(max_retries=3)
async def charge(order_id: int) -> None:
    task = current_task()
    log.info("charging %s", order_id, extra={"task_id": task and task.task_id})
```

It works from sync tasks too — the context rides along into the worker's thread — and
from anything the task calls. Outside a worker it returns `None`, like
`asyncio.current_task()`, so a shared logging helper can call it anywhere.

## Retries

When a task raises, ArdiQ retries it up to `max_retries` times before recording a failure.
Each attempt increments `tries` (visible on the [`TaskResult`](/reference/api/#taskresult)).

```python
@app.task(max_retries=3, backoff_ms=1000)
async def charge(order_id: int) -> None:
    # raises on a transient error → retried up to 3 times, 1s apart (then backoff grows)
    ...
```

A task that still fails after its last retry stores a failed `TaskResult` whose `value` is
the error's `repr`. To pick the delay from inside the task, or to report failures as they
happen, see [Handling failures](/guides/errors/).

## Timeouts

A `timeout` (in seconds) caps how long a single attempt may run. If it's exceeded the
attempt is cancelled and treated as a failure — so it follows the same retry rules:

```python
@app.task(timeout=10, max_retries=2)
async def call_flaky_api() -> dict:
    ...
```

The failed result's `value` reads `timed out after 10s`.

## Priorities

An app is created with a list of priority lanes, **lowest-first**:

```python
app = Ardiq(priorities=["low", "default", "high"])
```

Higher-priority lanes are consumed first. A task can declare a default lane, and any
individual enqueue can override it:

```python
@app.task(priority="high")
async def urgent(...): ...

# override per call
await urgent.options(priority="low").enqueue(...)
```

A task that names no lane goes to the **middle** one — `default` in the example above.
Set `default_priority` to choose it yourself:

```python
app = Ardiq(priorities=["low", "high"], default_priority="low")
```

With an even number of lanes the middle rounds *up*, so `["low", "high"]` defaults to
`high`: forgetting `priority=` should never quietly demote work, because demoted work
still completes and nothing tells you it happened. `app.default_priority` reports the
lane in force.

See [Enqueuing & scheduling](/guides/enqueuing/) for per-call overrides.

## Recurring tasks

To run a task on a schedule instead of on demand, register it with `@app.cron` (a cron
expression or an `every=` interval) — see [Recurring tasks](/guides/recurring/).

## Calling a task inline

A registered task is still a normal callable — calling it runs the function directly,
bypassing the queue entirely. This is handy in tests:

```python
result = await add(2, 3)   # runs now, in-process; no Redis involved
```

To actually dispatch it to a worker, use [`.enqueue(...)`](/guides/enqueuing/).
