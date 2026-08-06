<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/17tayyy/ardiq/main/docs/src/assets/ardiq-logo-dark.png">
    <img alt="ArdiQ" src="https://raw.githubusercontent.com/17tayyy/ardiq/main/docs/src/assets/ardiq-logo.png" width="260">
  </picture>
</p>

<p align="center">
  <a href="https://pypi.org/project/ardiq/"><img alt="PyPI version" src="https://img.shields.io/pypi/v/ardiq.svg"></a>
  <a href="https://pypi.org/project/ardiq/"><img alt="Python versions" src="https://img.shields.io/pypi/pyversions/ardiq.svg"></a>
  <a href="https://github.com/17tayyy/ardiq/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/17tayyy/ardiq/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://github.com/17tayyy/ardiq/blob/main/LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/License-MIT-blue.svg"></a>
</p>

<p align="center">
  <b><a href="https://ardiq.bytay.dev">Documentation</a></b> &nbsp;•&nbsp;
  <a href="https://ardiq.bytay.dev/guides/getting-started/">Getting started</a> &nbsp;•&nbsp;
  <a href="https://ardiq.bytay.dev/reference/api/">API reference</a> &nbsp;•&nbsp;
  <a href="https://ardiq.bytay.dev/guides/performance/">Benchmarks</a>
</p>

---

A fast distributed task queue with a **Rust core** and a clean **Python API**, backed by Redis streams.

ArdiQ runs the worker loop and all Redis I/O in Rust (via [PyO3](https://pyo3.rs) + [tokio](https://tokio.rs)); you write tasks in plain Python. The two meet at a single async callback, with the GIL held only for the microseconds it takes to start a task and read its result — so a single process handles high concurrency.

## Features

- 🦀 **Rust core** — the loop and Redis I/O run on tokio, off the GIL
- **Priority queues** — higher-priority tasks are consumed first
- **Delayed & scheduled** tasks (`delay_ms` / `schedule_ms`)
- **Cron & recurring** tasks (`@app.cron`) — 5-field cron (UTC) or `every=` intervals
- **Automatic retries** with quadratic backoff, configurable per task, or on demand (`raise Retry`)
- **Enqueue by name** (`app.send("task", ...)`) — producers never import the task module
- **Error hooks** (`@app.on_error`) — send every failed attempt to Sentry or your own reporter
- **Typed failures** (`BrokerError`) — catch "Redis is down" without a blanket `except`
- **Unique task names**, enforced at registration — a duplicate raises instead of silently shadowing
- **Crash recovery** — in-flight tasks of a dead worker are reclaimed (`XAUTOCLAIM`)
- **Results** with TTL, plus task **status** (`queued` / `running` / `complete` / `not_found`)
- **Abort/cancel** (`job.abort()`) — drops queued tasks and cancels running ones over pub/sub
- **Sync & async tasks** — blocking sync functions run in a thread pool
- **CLI worker** (`ardiq run module:app`) and **burst mode** (drain the queue and exit)

## Performance

Because the worker loop and every Redis round-trip run in Rust — off the GIL —
ArdiQ delivers **top-tier throughput at a fraction of the memory** of comparable
Python task queues.

Benchmarked head-to-head against arq, Taskiq, Streaq, Celery and Dramatiq on the
same machine (1,000 tasks, one worker, 10 concurrent):

| Queue        | Throughput      | Memory          |
|--------------|-----------------|-----------------|
| **ArdiQ** 🦀 | **top tier**    | **~34 MB** 🪶   |
| Taskiq       | top tier        | ~95 MB          |
| Streaq       | fast            | ~50 MB          |
| arq          | fast            | ~30 MB          |

- 🏆 **Among the fastest** async queues on both CPU- and I/O-bound workloads —
  effectively tied with the leader.
- 🪶 **Lightest in its class** — roughly a third of the memory of the next-fastest
  queue, and the lowest footprint of any queue at its performance level.
- 📈 **Near the theoretical ceiling** on I/O work — practically network-bound,
  with nothing lost to scheduling.
- 🎯 **Rock-steady** — negligible variance run to run.

> Throughput is shaped by hardware and workload, and the GIL caps in-process CPU
> work for *every* Python queue (ArdiQ included). The full, reproducible suite —
> with the honest caveats — lives in the
> [benchmark repo](https://github.com/17tayyy/python-task-queue-benchmarks).

## When to use ArdiQ

**Reach for ArdiQ when you want:**

- **High concurrency on a small footprint** — async-native, with the loop and
  Redis I/O in Rust, so one process does a lot without eating memory.
- **A modern, typed API** — `@app.task`, awaitable enqueue, `Job` handles,
  results and status built in.
- **Reliability out of the box** — priorities, retries with backoff, delayed and
  scheduled tasks, and crash recovery via Redis consumer groups.
- **Redis you already run** — no extra broker to operate.

**Consider the alternatives when:**

- **You need to saturate many CPU cores in one process** — like *every*
  single-process Python queue, ArdiQ runs your task body under the GIL, so
  CPU-bound work is serial per worker (scale out with more workers). For heavy
  CPU fan-out, a prefork model (Celery, Dramatiq) can be simpler.
- **You need a large, battle-tested ecosystem today** — Celery has years of
  integrations, schedulers, and dashboards. ArdiQ is young and moving fast.
- **You can't run Redis** — ArdiQ is Redis-only by design.

ArdiQ sits alongside **arq / Taskiq / Streaq** as a modern async queue — its edge
is the Rust core (memory and per-task overhead) and a batteries-included API.

## Installation

```console
$ pip install ardiq
```

That's everything — the library, the `ardiq` worker command, and a **single runtime
dependency** (`msgpack`). Define tasks, enqueue them, and run a worker either from
the CLI or from your own code (`await app.run()`).

You also need a Redis server — the quickest way is Docker:

```console
$ docker run -d --name ardiq-redis -p 6379:6379 redis
```

or install it from your package manager (or [redis.io](https://redis.io)).

> **Building from source** (if you want to hack on ArdiQ itself): you'll need [Rust](https://rustup.rs) and [uv](https://docs.astral.sh/uv/). Clone the repo and run `uv sync`.

## Quickstart

Define an app and some tasks (`example.py`):

```python
from ardiq import Ardiq

app = Ardiq(redis_url="redis://localhost:6379", queue_name="example")


@app.task()
async def add(a: int, b: int) -> int:
    return a + b


@app.task(max_retries=3)
def slow_double(x: int) -> int:   # sync task — runs in a thread
    return x * 2
```

Start a worker:

```console
$ ardiq run example:app
```

Enqueue tasks from anywhere and read their results:

```python
import asyncio
from example import add


async def main():
    job = await add.enqueue(2, 3)        # returns a Job handle
    print(job.id)
    print(await job.status())            # 'queued' | 'running' | 'complete'
    print(await job.result(timeout=5))   # waits → TaskResult(success=True, value=5, tries=1)


asyncio.run(main())
```

Or run the whole thing in one process with `python example.py`, which enqueues a
few tasks and processes them in burst mode.

## Enqueuing by name

The side that enqueues doesn't have to be the side that runs. `app.send` puts a
task on the queue by name, so a web service can dispatch work without importing
the task module — or its dependencies — at all:

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

Nothing is checked locally: the name is resolved by the worker that picks the
task up, and one it doesn't know fails there like any other error. For the
enqueue options, `app.ref` hands back the same handle `@app.task` returns:

```python
await queue.ref("build_report").options(delay_ms=60_000, priority="low").enqueue(7)
```

A `ref` can be enqueued but not called — there is no local function behind it.

**Priority does not travel with the name.** Everything else you put on
`@app.task(...)` — `max_retries`, `backoff_ms`, `timeout` — is applied by the
worker, which has the registry and can look it up. Priority is the exception: it
picks which stream the task goes into, so it is settled by the producer, before
the payload leaves. A task declared `@app.task(priority="high")` and dispatched
by name lands in the app's `default_priority` instead, with no warning — pass it
at the call site:

```python
await queue.ref("build_report", priority="high").enqueue(7)
```

Since the fallback is the middle lane, forgetting it is survivable rather than
disastrous, but the work still won't be where you declared it belongs.

## Retries and error hooks

A task that raises is retried up to `max_retries` times, waiting `tries²`
seconds between attempts (or the fixed `backoff_ms` you configure). Raise
`Retry` to make that call from inside the task instead:

```python
from ardiq import Retry


@app.task(max_retries=5)
async def call_api():
    response = await client.get(URL)
    if response.status_code == 429:
        raise Retry("rate limited", delay_ms=30_000)
    return response.json()
```

`Retry` still respects `max_retries`, so it can't loop forever; when the budget
runs out the task fails with it as the error.

`@app.on_error` runs a hook on every failed attempt, before ArdiQ decides
between retrying and failing — this is where a reporter like Sentry goes:

```python
import sentry_sdk


@app.on_error
def report(ctx):
    sentry_sdk.capture_exception(ctx.exc)
    log.warning("%s failed on try %s (retrying: %s)", ctx.name, ctx.tries, ctx.will_retry)
```

The hook takes an `ErrorContext(name, task_id, exc, tries, will_retry)`, may be
sync or async, and can be registered more than once — all of them run. One that
raises is logged and never changes the task's outcome.

It fires on timeouts, on every retry, and when a worker is handed a task it
doesn't know. It does **not** fire on abort, nor for a `Retry` you raised
yourself — only when that `Retry` finally gives up. Hooks run on the worker's
event loop, so keep them quick.

When *Redis* is what failed — unreachable, refusing or dropping connections — the
call raises `BrokerError` (`→ ArdiqError → RuntimeError`), so an enqueue in a
request handler can be caught precisely instead of with a bare `except
RuntimeError`:

```python
from ardiq import BrokerError

try:
    job = await queue.send("build_report", user_id)
except BrokerError:
    raise HTTPException(503, "queue unavailable")
```

## Shared resources (lifespan)

Tasks often need something expensive that should be built once per worker, not
per task — a database pool, an HTTP client. `@app.lifespan` registers an async
generator that sets up before the loop starts and tears down after it stops:

```python
@app.lifespan
async def lifespan():
    pool = await asyncpg.create_pool(DSN)
    yield {"db": pool}          # entries land on app.state
    await pool.close()


@app.task()
async def count_users() -> int:
    return await app.state.db.fetchval("select count(*) from users")
```

Yield a mapping to populate `app.state`, or yield nothing and assign
`app.state.db = ...` yourself. Either way `app.state` is available to async and
sync tasks alike.

The hook only runs inside `app.run()`, so a process that just enqueues never
opens the pool. Teardown runs even if the loop fails, and an exception during
setup stops the worker before it takes any work.

## Aborting tasks

`job.abort()` cancels a task whether it is waiting in the queue or already
running on some worker:

```python
job = await slow_report.options(delay_ms=60_000).enqueue()

if await job.abort():                # False if it already finished
    result = await job.result(timeout=5)
    print(result.aborted)            # True
    print(result.success)            # False
```

An aborted task ends as an ordinary failed `TaskResult` with `aborted` set, so
it never retries and `result(timeout=)` returns as soon as it settles. What
happens depends on where the task is when you call it:

| Where the task is | What abort does |
|---|---|
| Waiting on a delay or schedule | Dropped and finalized immediately. |
| Queued for pickup | The next worker to reach it skips it instead of running it. |
| Running | The worker holding it cancels it, within about a millisecond. |

Cancelling a **running** task needs a long-running worker: the worker subscribes
to the queue's abort channel while it runs, which `--burst` skips. Aborts are
still honored under burst, just not mid-flight.

Because cancellation is `asyncio` cancellation, a **sync** task can't be
interrupted mid-call — the worker stops waiting on it and reports it aborted,
but the thread runs to completion. Async tasks are cancelled at their next
`await`, so a task that swallows `CancelledError` keeps going.

## Recurring tasks

Register a task to run on a schedule with `@app.cron` — either a standard 5-field
cron expression (evaluated in **UTC**) or a fixed `every=` interval:

```python
@app.cron("0 3 * * *")            # daily at 03:00 UTC
async def nightly_report():
    ...


@app.cron(every=30)               # every 30s — int/float seconds or a timedelta
async def heartbeat():
    ...
```

Recurring tasks fire while a worker is running, and each occurrence is an ordinary
task with its own result, status, retries and timeout. The cron syntax is the
common subset — `*`, lists `,`, ranges `a-b`, and steps `*/n` — at minute
resolution; use `every=` for sub-minute schedules.

## Configuration

`Ardiq(...)` accepts:

| Option | Default | Description |
|---|---|---|
| `redis_url` | `redis://localhost:6379` | Redis connection URL |
| `queue_name` | `"default"` | Logical queue (key namespace) |
| `priorities` | `["default"]` | Priority names, **lowest-first** |
| `concurrency` | `16` | Max tasks running at once |
| `prefetch` | `concurrency * 2` | Max tasks held in memory (drives backpressure) |
| `idle_timeout_ms` | `60000` | When an unrenewed in-flight task may be reclaimed |
| `result_ttl_ms` | `300000` | How long results live (`0` drops, negative keeps forever) |
| `burst` | `False` | Exit once the queue drains |
| `serializer` / `deserializer` | msgpack | Wire codec; pass `pickle.dumps`/`pickle.loads` to send datetimes/objects |
| `cron_poll_s` | `1.0` | How often the worker restages due `@app.cron` occurrences |

`@app.task(...)` accepts `name`, `max_retries` (default 3), `backoff_ms`, `timeout` (seconds), and `priority`.
`@app.cron(spec, *, every=…, …)` takes those same per-task options plus the schedule.
Use `task.options(delay_ms=…, schedule_ms=…, priority=…, task_id=…).enqueue(...)` for one-off overrides.

## Logging

`ardiq run` configures Python's `logging` for the process (`INFO` by default, `DEBUG`
with `--verbose`/`-v`) and also initializes the Rust core's own logging at the same
level. Worker lifecycle (`worker starting`, `worker stopped`) logs at `INFO`; task
lifecycle logs at `DEBUG` (`task started`, `task succeeded`) through `WARN` (`task
retry scheduled`) and `ERROR` (`task failed`, `task unknown`). Task args, kwargs,
and results are never logged.

Logging inside a task is just standard `logging` — it works the same for async tasks
and for sync tasks run via `asyncio.to_thread`:

```python
import logging

logger = logging.getLogger(__name__)


@app.task()
async def send_email(to: str) -> None:
    logger.info("sending email to %s", to)
    ...
```

If you embed `Ardiq` outside the `ardiq` CLI, call `logging.basicConfig(...)` yourself
(see `example.py`).

## Development

```console
$ docker compose up -d      # Redis on localhost:6379
$ uv run pytest             # test suite (needs Redis)
$ uv run ruff check .       # lint
$ uv run ty check ardiq tests   # type-check
```

After changing the Rust core, rebuild with `uv sync --reinstall-package ardiq`.

## Contributing

Bug reports, docs fixes and features are all welcome — see
[CONTRIBUTING.md](CONTRIBUTING.md) for setup, the layout of the codebase and
what to open an issue about first. Questions and ideas go in
[Discussions](https://github.com/17tayyy/ardiq/discussions).

## License

[MIT](LICENSE)
