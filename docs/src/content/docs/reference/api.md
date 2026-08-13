---
title: Python API
description: Reference for the public ArdiQ API — Ardiq, Task, Job, TaskResult and TaskInfo.
---

Everything public is importable from the top-level package:

```python
from ardiq import Ardiq, Task, Job, TaskResult, TaskInfo
```

## `Ardiq`

The app: owns the Rust core, the task registry, and the wire codec.

```python
Ardiq(
    redis_url: str | None = None,
    queue_name: str = "default",
    priorities: list[str] | None = None,
    *,
    serializer: Callable[[Any], bytes] | None = None,
    deserializer: Callable[[bytes], Any] | None = None,
    cron_poll_s: float = 1.0,
    **core_kwargs: Any,
)
```

Constructor arguments are documented in [Configuration](/reference/configuration/)
(`core_kwargs` covers `concurrency`, `prefetch`, `idle_timeout_ms`, `result_ttl_ms`,
`burst`).

### Properties

| Property | Type | Description |
|---|---|---|
| `worker_id` | `str` | This worker's id (set by the core). |
| `default_priority` | `str` | The lane a task with no `priority` lands in. |
| `burst` | `bool` | Read/write; when `True` the loop exits once the queue drains. |
| `tasks` | `list[str]` | Names of the registered tasks. |

### `task(...)`

Decorator that registers a task and returns a [`Task`](#task).

```python
def task(
    fn: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    max_retries: int = 3,
    backoff_ms: int = 0,
    timeout: float | None = None,
    priority: str | None = None,
    unique: bool = False,
) -> Task
```

Usable bare (`@app.task`) or called (`@app.task(max_retries=5)`). See
[Defining tasks](/guides/tasks/).

With `unique=True`, enqueuing a call identical to one already waiting or running
returns that job instead of creating a second one — see
[Unique tasks](/guides/enqueuing/#unique-tasks).

### `cron(...)`

Register a recurring task and return a [`Task`](#task); see
[Recurring tasks](/guides/recurring/).

```python
def cron(
    spec: str | None = None,
    *,
    every: timedelta | float | None = None,
    name: str | None = None,
    max_retries: int = 3,
    backoff_ms: int = 0,
    timeout: float | None = None,
    priority: str | None = None,
) -> Callable[..., Task]
```

Pass exactly one of `spec` (a 5-field cron expression, UTC) or `every` (seconds or a
`timedelta`).

### Methods

| Method | Returns | Description |
|---|---|---|
| `await run()` | `None` | Start the worker loop; runs until `stop()` or (in burst) the queue drains. |
| `stop()` | `None` | Ask the loop to shut down gracefully. |
| `await send(name, *args, **kwargs)` | `Job` | Enqueue by name, with no local registration; see [Enqueuing by name](/guides/enqueuing/#enqueuing-by-name). |
| `ref(name, *, priority=None, unique=False)` | `Task` | A handle to a task registered elsewhere — enqueueable, not callable. |
| `await queue_size()` | `int` | Number of jobs waiting across lanes. |
| `await result(task_id, timeout=None)` | `TaskResult \| None` | Fetch a result; with `timeout` (s) waits, else returns now-or-`None`. |
| `await status(task_id)` | `str` | `queued` / `scheduled` / `running` / `complete` / `not_found`. |
| `await info(task_id)` | `TaskInfo \| None` | Snapshot of an unfinished task, else `None`. |
| `await abort(task_id)` | `bool` | Cancel a queued or running task; `False` if already finished. |
| `lifespan(fn)` | decorator | Register worker startup/shutdown; see [Shared resources](/guides/lifespan/). |
| `on_error(fn)` | decorator | Register a failure hook; see [Handling failures](/guides/errors/). |
| `state` | `State` | Worker-scoped resources set by the lifespan hook. |

## `Task`

A registered task, returned by `@app.task` (or by [`app.ref`](#ardiq), without a function).
Call it to run inline; use its async methods to dispatch.

Generic as `Task[**P, R]` over the decorated function's signature, so `enqueue` and
inline calls are type-checked against it — see
[Type checking](/guides/tasks/#type-checking). A `ref` types as `Task[..., Any]`.

| Member | Description |
|---|---|
| `name` | The registered name. |
| `fn` | The underlying function, or `None` for a `ref`. |
| `priority` | The task's default priority lane (or `None`). |
| `unique` | Whether an identical call already in flight is reused instead of enqueued again. |
| `task(*args, **kwargs)` | Calling the `Task` runs `fn` **inline**, bypassing the queue. A `ref` raises `TypeError`. |
| `await enqueue(*args, **kwargs)` | Dispatch to a worker; returns a [`Job`](#job). |
| `options(...)` | Returns a bound task with per-call overrides; see below. |

### `options(...)`

```python
def options(
    *,
    task_id: str | None = None,
    priority: str | None = None,
    delay_ms: int = 0,
    schedule_ms: int = 0,
    expire_ms: int = 0,
    unique: bool | None = None,
) -> _BoundTask
```

Returns an object with the same `await enqueue(*args, **kwargs)` method, carrying the
overrides. See [Enqueuing & scheduling](/guides/enqueuing/#per-call-options).

```python
await add.options(priority="high", delay_ms=5000).enqueue(2, 3)
```

### `task.prepare(*args, **kwargs)`

```python
def prepare(*args, **kwargs) -> PreparedTask
```

The call `.enqueue` would make, held back so `enqueue_many` can send a batch in
one round trip. Available on `.options(...)` too. Arguments are checked against
the task's signature exactly as `.enqueue` checks them.

### `await app.enqueue_many(tasks)`

```python
async def enqueue_many(tasks: Iterable[PreparedTask]) -> list[Job]
```

Sends prepared tasks in one round trip and returns their `Job`s in order. Any
iterable works, including a generator. Priorities are validated across the whole
batch before anything is sent. See
[Enqueuing in bulk](/guides/enqueuing/#enqueuing-in-bulk).

```python
jobs = await queue.enqueue_many(charge.prepare(oid) for oid in order_ids)
```

## `Job`

An immutable handle to an enqueued task — just the app plus an id.

| Member | Returns | Description |
|---|---|---|
| `app` | `Ardiq` | The owning app. |
| `id` | `str` | The job id. |
| `await result(timeout=None)` | `TaskResult \| None` | Fetch the result; with `timeout` (s) waits, raising `TimeoutError`. |
| `await status()` | `str` | Current status. |
| `await info()` | `TaskInfo \| None` | Snapshot if unfinished, else `None`. |
| `await abort()` | `bool` | Cancel the task; `False` if already finished. See [Aborting tasks](/guides/aborting/). |

## `TaskResult`

A `NamedTuple` describing a finished task.

| Field | Type | Description |
|---|---|---|
| `success` | `bool` | Whether the task returned (vs failed after retries). |
| `value` | `Any` | Return value on success; error `repr` on failure. |
| `tries` | `int` | Number of attempts. |
| `enqueue_time` | `int` | Epoch ms when enqueued. |
| `start` | `int` | Epoch ms when execution started. |
| `finish` | `int` | Epoch ms when execution finished. |
| `duration_ms` | `int` | Property: `finish - start`. |
| `aborted` | `bool` | Property: the task was cancelled rather than failing on its own. |

## `State`

A namespace holding worker-scoped resources, reachable as `app.state`. Populated by the
`@app.lifespan` hook — either from the mapping it yields or by direct assignment. Reading
an attribute that was never set raises `AttributeError` naming it.

```python
app.state.db          # set by the lifespan hook
```

## `TaskInfo`

A `NamedTuple` snapshot of an unfinished task (queued, scheduled, or running).

| Field | Type | Description |
|---|---|---|
| `task_id` | `str` | The job id. |
| `fn_name` | `str` | Registered task name. |
| `args` | `tuple` | Positional arguments. |
| `kwargs` | `dict` | Keyword arguments. |
| `enqueue_time` | `int` | Epoch ms when enqueued. |
| `tries` | `int` | Attempts so far. |
| `status` | `str` | Current status. |
| `scheduled_at` | `int \| None` | Epoch ms if waiting in the delayed set, else `None`. |

## `ErrorContext`

A `NamedTuple` handed to every `@app.on_error` hook; see
[Handling failures](/guides/errors/#reporting-errors).

| Field | Type | Description |
|---|---|---|
| `name` | `str` | The task's registered name. |
| `task_id` | `str` | The job id. |
| `exc` | `BaseException` | The exception the attempt raised. |
| `tries` | `int` | The attempt that just failed, counting from 1. |
| `will_retry` | `bool` | Whether another attempt is coming. |

## `TaskContext`

A `NamedTuple` describing the task running right now, returned by `current_task()` —
`None` outside a worker. See [Knowing which task you are](/guides/tasks/#knowing-which-task-you-are).

| Field | Type | Description |
|---|---|---|
| `task_id` | `str` | The job id. |
| `name` | `str` | The task's registered name. |
| `tries` | `int` | The attempt in progress, counting from 1. |

## `ArdiqError` / `BrokerError`

`BrokerError` → `ArdiqError` → `RuntimeError`. `BrokerError` means Redis was
unreachable; `ArdiqError` is anything else the core raises. See
[When the broker itself fails](/guides/errors/#when-the-broker-itself-fails).

## `Retry`

An exception a task raises to run again, optionally after a delay of its choosing. It
respects `max_retries`; see [Retrying on demand](/guides/errors/#retrying-on-demand).

```python
raise Retry("rate limited", delay_ms=30_000)
```

| Argument | Type | Default | Description |
|---|---|---|---|
| `message` | `str` | `"retry requested"` | Why, recorded as the error if the retries run out. |
| `delay_ms` | `int \| None` | `None` | Wait this long before the next attempt; `None` uses the task's backoff. |
