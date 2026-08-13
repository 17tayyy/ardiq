---
title: Running a worker
description: Start workers with the ardiq CLI, drain the queue with burst mode, and shut down gracefully.
---

import { Tabs, TabItem } from '@astrojs/starlight/components';

A worker is a process that loads your `Ardiq` app, connects to Redis, and runs the loop —
pulling tasks and executing them. The usual way to start one is the CLI.

## The CLI

The `ardiq` command comes with the base install. To start a worker from inside your own
process instead, see [Running in code](#running-in-code) below.

```console
$ ardiq run example:app
```

The argument is an **import path** of the form `module:attribute`, where `attribute` is
your `Ardiq` instance. ArdiQ imports the module (so all `@app.task` decorators register)
and runs that app.

| Option        | Description |
|---------------|-------------|
| `--burst`, `-b` | Process everything currently queued, then exit. |
| `--verbose`, `-v` | DEBUG-level logging, including the Rust core's logs. |
| `--quiet`, `-q` | Skip the startup banner and log a single plain line instead. |

```console
$ ardiq run example:app --verbose
$ ardiq run example:app --burst
$ ardiq run example:app --quiet     # for CI and log collectors
```

:::note
The module must be importable from where you launch the worker — make sure it's on your
`PYTHONPATH` (running from the project root usually does it).
:::

## Burst mode

Burst mode drains the queue and exits instead of waiting for more work. It's ideal for
tests, cron-style batch runs, and single-file demos. You can enable it from the CLI
(`--burst`) or in code:

```python
app.burst = True
await app.run()    # returns once the queue is empty
```

## Running in code

You don't have to use the CLI. Any process can run the loop directly:

```python
import asyncio
from example import app


async def main() -> None:
    await app.run()       # runs until app.stop() is called


asyncio.run(main())
```

Call `app.stop()` (e.g. from a signal handler or another task) to ask the loop to wind
down gracefully.

## Graceful shutdown

The CLI installs handlers for **SIGINT** and **SIGTERM** that call `app.stop()`, so
`Ctrl-C` or a `docker stop` lets in-flight tasks settle before the process exits. If you
run the loop yourself and want the same behavior, wire it up:

```python
import asyncio
import signal
from example import app


async def main() -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, app.stop)
    await app.run()


asyncio.run(main())
```

## Logging

`ardiq run` configures Python's `logging` for the process — `INFO` by default, `DEBUG`
with `--verbose` — and initializes the Rust core's logging at the same level, so both
surface on stderr.

| Level | What you see |
|---|---|
| `INFO` | `worker starting` and `worker stopped` (with a `reason` of `signal`, `burst` or `unknown`), plus tasks aborted before they ran. |
| `DEBUG` | `task started` and `task succeeded`, with `duration_ms`. |
| `WARN` | `task retry scheduled` (with `delay_ms`) and `task aborted` mid-flight. |
| `ERROR` | `task failed` after the last retry, `task unknown`, and internal errors. |

Every task line carries the same key-value fields — `id=`, `name=`, `worker=`, `try=` —
so they're easy to grep or parse. Arguments, keyword arguments and return values are
**never** logged.

### Logging from inside a task

Task bodies use standard `logging`, with no special setup and nothing intercepted or
swallowed. This works the same in async tasks and in sync tasks running in a thread:

```python
import logging

logger = logging.getLogger(__name__)


@app.task()
async def send_email(to: str) -> None:
    logger.info("sending email to %s", to)
```

If you embed `Ardiq` outside the `ardiq` CLI (see [Running in code](#running-in-code)),
call `logging.basicConfig(...)` yourself — otherwise Python's default configuration drops
anything below `WARNING`.

## Concurrency & scaling

A single worker runs up to `concurrency` tasks at once (default 16) and holds up to
`prefetch` in memory for backpressure — see [Configuration](/reference/configuration/).

Because task bodies run under the GIL, **scale CPU-bound work by running more worker
processes** against the same queue. Multiple workers form a Redis consumer group, so jobs
are distributed across them and a crashed worker's in-flight tasks are reclaimed
automatically.

`--workers N` starts N of them for you and supervises the lot:

```console
# four workers sharing one queue
$ ardiq run example:app --workers 4
```

One banner is printed, each child logs under its own `worker_id`, and `Ctrl-C`
or a `SIGTERM` to the supervisor reaches every worker. If one of them exits
non-zero the supervisor stops the rest and exits with that code, so a crashed
worker fails the deployment instead of leaving it quietly running short-handed.

Nothing is shared between the processes — they are independent consumers of the
same streams — so starting them yourself, one per container, works exactly as
well and is what an orchestrator will do anyway:

```console
$ ardiq run example:app &
$ ardiq run example:app &
```
