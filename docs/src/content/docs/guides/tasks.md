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
workers, or run the task in a process of its own.
:::

## Running a task in its own process

A thread shares the worker's memory and can't be stopped from outside. For heavy sync
work, such as processing a large image with GDAL or numpy, pass `executor="process"`:

```python
@app.task(executor="process", timeout=45 * 60)
def process_scene(scene_id: str) -> dict:
    ...
```

Each run then starts a fresh Python process that imports the task's module, calls the
function and exits:

- **A timeout or an abort stops it.** The process is terminated, and killed if it does
  not exit within 5 seconds, instead of running on after the worker gave up.
- **Its memory goes back to the system** when it exits, so allocator arenas and library
  caches from one run don't stay in the worker for the next.
- **It doesn't share the worker's GIL**, so CPU-bound work runs alongside the worker's
  other tasks.
- **Its logs reach the worker.** Records it emits are handled by the worker's logging, so
  they come out with the same format and level as everything else.
- **It dies with the worker.** If the worker is killed outright, the process notices and
  exits within a second or so.

Arguments and the return value cross between the processes by pickle, so they must
pickle: anything that came through the serializer does. An exception raised in the task
comes back as itself, with the child's traceback in a note, and follows the usual retry
rules; `Retry` works as in a thread. If the process dies without an answer, for example
because the system killed it for running out of memory, the attempt fails with
`ChildProcessError` and its exit code.

Middleware, error hooks, retries and metrics still run in the worker. Inside the task,
`current_task()` works, but `app.state` is empty: the [lifespan](/guides/lifespan/) runs
in the worker, not in each process, so open what the task needs inside it.

Starting a process costs an interpreter start plus importing the task's module, from a
fraction of a second up to a couple with heavy libraries. That is nothing next to a task
that runs for minutes, and too much for one that runs for milliseconds: keep short tasks
in the default `executor="thread"`.

The task must be a module-level `def`: the process imports it by name. An `async def`, a
lambda or a nested function is refused when you register it. If you start the worker
from your own script rather than `ardiq run`, guard the start with
`if __name__ == "__main__":`, as with any `multiprocessing` code, since each process
imports that script again.

## Decorator options

`@app.task(...)` accepts:

| Option        | Type          | Default | Description |
|---------------|---------------|---------|-------------|
| `name`        | `str`         | function name | The name used on the wire and in the registry. Required if the callable has no `__name__`. |
| `max_retries` | `int`         | `3`     | How many times to retry after the first attempt fails. |
| `backoff_ms`  | `int`         | `0`     | Delay between retries in ms. `0` uses the core's default backoff. |
| `timeout`     | `float \| None` | `None` | Per-task timeout in **seconds**. A task that exceeds it fails (and may retry). |
| `priority`    | `str \| None` | `None`  | Default priority lane for this task (see [Priorities](#priorities)). |
| `executor`    | `"thread" \| "process"` | `"thread"` | Where a sync task runs (see [Running a task in its own process](#running-a-task-in-its-own-process)). |

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

A sync task in a thread can't be interrupted: the attempt fails on time, but the function
runs on in its thread until it returns. If it must actually stop, give it
[a process of its own](#running-a-task-in-its-own-process).

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

A lane that isn't in `priorities` raises, both at registration and at enqueue:

```python
app = Ardiq(priorities=["low", "default", "high"])

@app.task(priority="urgent")   # ValueError: 'urgent' is not one of
async def charge(...): ...     # ['low', 'default', 'high'] — no worker reads that lane
```

:::caution[Producers must declare the same lanes as the worker]
The check runs wherever a priority is given, including `app.send` and `ref().enqueue`
in a process that only produces. A web app declaring `Ardiq(priorities=["default"])`
cannot enqueue to `"high"` even if the worker has that lane — which is the point: it
was writing to a stream nothing consumed. Declare the full list on both sides.
:::

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

## Type checking

`Task` carries the decorated function's signature, so `.enqueue(...)` takes the same
arguments the task does — a mistake is caught by mypy, pyright or ty rather than by a
worker at 3am:

```python
@app.task()
async def charge(user_id: int, amount: float) -> str: ...

await charge.enqueue(1, 9.99)          # ok
await charge.enqueue("1", 9.99)        # error: str is not int
await charge.enqueue(1)                # error: missing 'amount'
```

`.options(...)` keeps the signature, so `charge.options(delay_ms=5000).enqueue(...)` is
checked too. Calling the task inline returns its declared return type.

Tasks reached by name are the exception: [`app.send`](/guides/enqueuing/#enqueuing-by-name)
and `app.ref` have no local function to read a signature from, so their arguments are
unchecked by construction.
