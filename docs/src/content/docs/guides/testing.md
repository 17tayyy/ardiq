---
title: Testing
description: Run tasks inline in your tests with ardiq.testing.inline, with no Redis and no worker.
---

`ardiq.testing.inline` runs a task the moment it is enqueued, in memory. Your tests need
no Redis and no worker, and by the time `enqueue` returns the result is already there.

```python
from ardiq.testing import inline

from myapp.tasks import app, add


async def test_add():
    async with inline(app):
        job = await add.enqueue(2, 3)
        result = await job.result()
        assert result.success
        assert result.value == 5
```

Leave the block and the app talks to Redis again, so the same `app` object works in tests
and in production.

## What behaves as on a worker

The task runs through the same code a worker uses. In particular:

- **Retries** follow `max_retries`, and `raise Retry(...)` works.
- **Timeouts** apply, and a slow task fails with `timed out after ...`.
- **`@app.on_error` hooks** fire on every failed attempt.
- **`current_task()`** returns the task's id, name and try number.
- **Arguments and results go through the serializer**, so a tuple comes back as a list
  and an argument msgpack cannot encode raises at enqueue, just as in production.
- **An unknown task** (from `app.send` or `app.ref`) ends as a failed result.
- **Priorities** are still checked, so a lane no worker reads raises `ValueError`.

A failed task does not raise in your test. It ends as a failed `TaskResult`, the same
thing a worker would store, so assert on `result.success` and `result.value`.

## What is different

- **Retries run back to back.** `backoff_ms` and `Retry(delay_ms=)` are ignored, so a
  test never sleeps.
- **Delays and schedules are ignored.** `delay_ms` and `schedule_ms` run the task at once.
- **`result(timeout=)` never waits.** The task has already finished, so it returns the
  result or raises `TimeoutError` straight away.
- **`queue_size()` is always 0**, and `app.run()` raises, because there is no queue to
  work through.
- **Cron tasks do not fire** on their own. Enqueue one to test it:
  `await nightly_report.enqueue()`.

## Shared resources

The `@app.lifespan` hook runs around the block, so tasks that use `app.state` work as they
do on a worker. To swap a resource for a fake, set it inside the block:

```python
async def test_report_uses_the_db():
    async with inline(app):
        app.state.db = FakeDB()
        job = await build_report.enqueue(42)
        assert (await job.result()).success
```

## A pytest fixture

With `pytest-asyncio`, a fixture puts every test in inline mode:

```python
import pytest

from ardiq.testing import inline
from myapp.tasks import app


@pytest.fixture
async def tasks():
    async with inline(app):
        yield app
```
