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
) -> Task
```

Usable bare (`@app.task`) or called (`@app.task(max_retries=5)`). See
[Defining tasks](/guides/tasks/).

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
| `await queue_size()` | `int` | Number of jobs waiting across lanes. |
| `await result(task_id, timeout=None)` | `TaskResult \| None` | Fetch a result; with `timeout` (s) waits, else returns now-or-`None`. |
| `await status(task_id)` | `str` | `queued` / `scheduled` / `running` / `complete` / `not_found`. |
| `await info(task_id)` | `TaskInfo \| None` | Snapshot of an unfinished task, else `None`. |

## `Task`

A registered task, returned by `@app.task`. Call it to run inline; use its async methods to
dispatch.

| Member | Description |
|---|---|
| `name` | The registered name. |
| `fn` | The underlying function. |
| `priority` | The task's default priority lane (or `None`). |
| `task(*args, **kwargs)` | Calling the `Task` runs `fn` **inline**, bypassing the queue. |
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
) -> _BoundTask
```

Returns an object with the same `await enqueue(*args, **kwargs)` method, carrying the
overrides. See [Enqueuing & scheduling](/guides/enqueuing/#per-call-options).

```python
await add.options(priority="high", delay_ms=5000).enqueue(2, 3)
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
