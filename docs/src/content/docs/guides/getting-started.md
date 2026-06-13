---
title: Getting started
description: Install ArdiQ, start Redis, write your first task, and run a worker.
---

## Requirements

- **Python 3.13+**
- A **Redis** server (ArdiQ uses Redis streams as its broker and result store).

## Install

```console
$ pip install ardiq
```

ArdiQ ships as a prebuilt wheel with the Rust core baked in — no Rust toolchain needed to
*use* it. The base package is the library and has a single runtime dependency (`msgpack`):
enough to define tasks, enqueue, and run a worker from your own code.

The `ardiq` **worker command** used below ships in the `cli` extra:

```console
$ pip install 'ardiq[cli]'
```

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
