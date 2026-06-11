---
title: Running a worker
description: Start workers with the ardiq CLI, drain the queue with burst mode, and shut down gracefully.
---

import { Tabs, TabItem } from '@astrojs/starlight/components';

A worker is a process that loads your `Ardiq` app, connects to Redis, and runs the loop —
pulling tasks and executing them. The usual way to start one is the CLI.

## The CLI

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

```console
$ ardiq run example:app --verbose
$ ardiq run example:app --burst
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

## Concurrency & scaling

A single worker runs up to `concurrency` tasks at once (default 16) and holds up to
`prefetch` in memory for backpressure — see [Configuration](/reference/configuration/).

Because task bodies run under the GIL, **scale CPU-bound work by running more worker
processes** against the same queue. Multiple workers form a Redis consumer group, so jobs
are distributed across them and a crashed worker's in-flight tasks are reclaimed
automatically.

```console
# three workers sharing one queue
$ ardiq run example:app &
$ ardiq run example:app &
$ ardiq run example:app &
```
