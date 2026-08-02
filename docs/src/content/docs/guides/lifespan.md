---
title: Shared resources
description: Build a database pool or HTTP client once per worker with @app.lifespan and reach it from tasks via app.state.
---

Most real tasks need something expensive that should exist **once per worker**, not once
per task — a database pool, an HTTP client, a model loaded into memory. `@app.lifespan`
registers a hook that sets those up before the worker takes any work and tears them down
after it stops.

```python
import asyncpg
from ardiq import Ardiq

app = Ardiq()


@app.lifespan
async def lifespan():
    pool = await asyncpg.create_pool(DSN)
    yield {"db": pool}          # entries land on app.state
    await pool.close()


@app.task()
async def count_users() -> int:
    return await app.state.db.fetchval("select count(*) from users")
```

The hook is an **async generator with exactly one `yield`**: everything before it is
startup, everything after is shutdown.

## Reaching resources from a task

Whatever the hook yields must be a mapping, and its entries become attributes on
[`app.state`](/reference/api/#state). If you'd rather assign them yourself, yield nothing:

```python
@app.lifespan
async def lifespan():
    app.state.http = httpx.AsyncClient()
    yield
    await app.state.http.aclose()
```

`app.state` works the same from **async and sync tasks** — a sync task runs in a thread,
but it's the same object.

```python
@app.task()
def render(template: str) -> str:      # sync task, runs in a thread
    return app.state.jinja.get_template(template).render()
```

Reading a key that was never set raises `AttributeError` naming the key, so a typo or a
forgotten hook fails loudly instead of surfacing as `None` deep inside a task.

## When it runs

The hook runs **inside `app.run()`** — so a web process that only enqueues never opens the
pool, even though it imports the same app.

- Startup completes before the worker pulls its first task.
- Shutdown runs after the loop stops, including when it stops because of an error, so
  `finally` blocks and `await pool.close()` are reliable.
- An exception during startup propagates out of `run()` and the worker never starts.

:::note[Burst mode too]
Unlike cron and abort watching, the lifespan runs in `--burst` as well — a batch worker
needs its database pool just as much as a long-running one.
:::

## One hook per app

`@app.lifespan` registers a single hook; applying it twice replaces the first. To set up
several things, do them in the one hook — `contextlib.AsyncExitStack` keeps the teardown
tidy:

```python
from contextlib import AsyncExitStack


@app.lifespan
async def lifespan():
    async with AsyncExitStack() as stack:
        pool = await stack.enter_async_context(asyncpg.create_pool(DSN))
        http = await stack.enter_async_context(httpx.AsyncClient())
        yield {"db": pool, "http": http}
```
