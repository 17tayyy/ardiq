---
title: Getting started
description: Install ArdiQ, start Redis, write your first task, and run a worker.
---

## Requirements

- **Python 3.12+** (3.13+ recommended — see the note below)
- A **Redis** server (ArdiQ uses Redis streams as its broker and result store).

:::caution[On Python 3.12, a process that enqueues can crash as it exits]
ArdiQ's core keeps Rust threads that outlive your code. When CPython 3.12 tears
the interpreter down, one of those threads can release a Python reference a
moment too late and the process dies with a segmentation fault — **after** its
work is done. Nothing is lost: every task is enqueued, run and stored before
this can happen; only the exit code is wrong.

It needs many processes exiting at once to show up at all (measured: 12% of 100
simultaneous producers, never once when run one at a time), and **Python 3.13
and up are unaffected** — CPython gained the check that PyO3 needs to refuse the
unsafe release. The `ardiq` worker command is not affected on any version: it
exits without finalizing the interpreter, precisely because of this.

So if you fan out short-lived processes that enqueue and exit — cron jobs,
scripts, CI steps — prefer 3.13+. The upstream gap is tracked in
[pyo3-async-runtimes#40](https://github.com/PyO3/pyo3-async-runtimes/issues/40).
:::

## Install

```console
$ pip install ardiq
```

ArdiQ ships as a prebuilt wheel with the Rust core baked in — no Rust toolchain needed to
*use* it. That single install gives you the library, the `ardiq` worker command used
below, and one runtime dependency (`msgpack`).

You also need a Redis server — the quickest way is Docker:

```console
$ docker run -d --name ardiq-redis -p 6379:6379 redis   # Redis on localhost:6379
```

or install it from your package manager (or [redis.io](https://redis.io)).

:::note[Building from source]
If you want to hack on ArdiQ itself you'll need [Rust](https://rustup.rs) and
[uv](https://docs.astral.sh/uv/). Clone the repo and run `uv sync`; after changing the Rust
core, rebuild with `uv sync --reinstall-package ardiq`.
:::

## Your first task

<Steps>

1. **Define an app and some tasks** in a module — say `example.py`:

   ```python title="example.py"
   from ardiq import Ardiq

   app = Ardiq(redis_url="redis://localhost:6379", queue_name="example")


   @app.task()
   async def add(a: int, b: int) -> int:
       return a + b


   @app.task(max_retries=3)
   def slow_double(x: int) -> int:   # sync task — runs in a thread
       return x * 2
   ```

2. **Start a worker** that loads `app` from `example.py`:

   ```console
   $ ardiq run example:app
   ```

3. **Enqueue tasks** from anywhere — a web handler, a script, a REPL — and read their
   results:

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

</Steps>

## All in one process

You don't need a separate worker to try things out. **Burst mode** drains the queue and
exits, so you can enqueue and process in a single script:

```python title="example.py"
import asyncio
from ardiq import Ardiq

app = Ardiq(redis_url="redis://localhost:6379", queue_name="example")


@app.task()
async def add(a: int, b: int) -> int:
    return a + b


async def main() -> None:
    jobs = [await add.enqueue(i, i) for i in range(3)]

    app.burst = True
    await app.run()                # process everything queued, then exit

    for job in jobs:
        print(await job.result())


if __name__ == "__main__":
    asyncio.run(main())
```

```console
$ python example.py
```

## Where to go next

- [Defining tasks](/guides/tasks/) — retries, timeouts, priorities, sync vs async.
- [Enqueuing & scheduling](/guides/enqueuing/) — delays, scheduled runs, per-call options.
- [Results & introspection](/guides/results/) — `Job`, `TaskResult`, `status()`, `info()`.
- [Running a worker](/guides/worker/) — the CLI, burst mode, graceful shutdown.
