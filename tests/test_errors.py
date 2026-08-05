"""The `@app.on_error` hook: when it fires, what it gets, and how it fails."""

import asyncio
import logging

from ardiq.models import ErrorContext


def _pack(app, fn_name, args=(), kwargs=None):
    return app._dumps({"f": fn_name, "a": list(args), "k": kwargs or {}, "t": 0})


def _collect(app) -> list[ErrorContext]:
    seen: list[ErrorContext] = []

    @app.on_error
    def record(ctx):
        seen.append(ctx)

    return seen


async def test_hook_fires_on_a_failing_attempt(make_app):
    app = make_app("err_basic")
    seen = _collect(app)

    @app.task(max_retries=0)
    async def boom():
        raise ValueError("nope")

    await app._execute("t1", _pack(app, "boom"), 1)

    assert len(seen) == 1
    ctx = seen[0]
    assert ctx.name == "boom"
    assert ctx.task_id == "t1"
    assert isinstance(ctx.exc, ValueError) and str(ctx.exc) == "nope"
    assert ctx.tries == 1
    assert ctx.will_retry is False


async def test_will_retry_tracks_the_retry_budget(make_app):
    app = make_app("err_budget")
    seen = _collect(app)

    @app.task(max_retries=1, backoff_ms=1)
    async def boom():
        raise RuntimeError("dead")

    await app._execute("t1", _pack(app, "boom"), 1)  # one retry left
    await app._execute("t1", _pack(app, "boom"), 2)  # budget spent

    assert [c.will_retry for c in seen] == [True, False]
    assert [c.tries for c in seen] == [1, 2]


async def test_async_hooks_are_awaited(make_app):
    app = make_app("err_async")
    seen: list[str] = []

    @app.on_error
    async def record(ctx):
        await asyncio.sleep(0)
        seen.append(ctx.name)

    @app.task(max_retries=0)
    async def boom():
        raise ValueError("x")

    await app._execute("t1", _pack(app, "boom"), 1)

    assert seen == ["boom"]


async def test_every_hook_runs(make_app):
    app = make_app("err_many")
    calls: list[str] = []

    @app.on_error
    def first(ctx):
        calls.append("first")

    @app.on_error
    async def second(ctx):
        calls.append("second")

    @app.task(max_retries=0)
    async def boom():
        raise ValueError("x")

    await app._execute("t1", _pack(app, "boom"), 1)

    assert calls == ["first", "second"]


async def test_a_hook_that_raises_is_logged_and_ignored(make_app, caplog):
    app = make_app("err_hook_boom")

    @app.on_error
    def broken(ctx):
        raise KeyError("hook is buggy")

    seen = _collect(app)  # registered after the broken one

    @app.task(max_retries=0)
    async def boom():
        raise ValueError("original")

    with caplog.at_level(logging.ERROR, logger="ardiq"):
        outcome, env, _ = await app._execute("t1", _pack(app, "boom"), 1)

    assert outcome == 1  # FAILURE, unchanged
    result = app._unpack(env)
    assert result is not None and result.value == "ValueError('original')"
    assert len(seen) == 1  # a broken hook doesn't stop the next one
    assert any("on_error hook failed" in r.message for r in caplog.records)


async def test_hook_fires_on_timeout(make_app):
    app = make_app("err_timeout")
    seen = _collect(app)

    @app.task(timeout=0.05, max_retries=0)
    async def slow():
        await asyncio.sleep(30)

    await app._execute("t1", _pack(app, "slow"), 1)

    assert len(seen) == 1
    assert isinstance(seen[0].exc, TimeoutError)


async def test_hook_fires_for_an_unknown_task(make_app):
    app = make_app("err_unknown")
    seen = _collect(app)

    await app._execute("t1", _pack(app, "nowhere"), 1)

    assert len(seen) == 1
    ctx = seen[0]
    assert isinstance(ctx.exc, LookupError)
    assert ctx.name == "nowhere" and ctx.will_retry is False


async def test_hook_does_not_fire_on_abort_at_pickup(make_app):
    app = make_app("err_abort_pickup")
    seen = _collect(app)

    @app.task()
    async def work():
        return "ran"

    await app._execute("t1", _pack(app, "work"), 1, True)

    assert seen == []


async def test_hook_does_not_fire_when_a_running_task_is_cancelled(make_app):
    app = make_app("err_abort_inflight")
    seen = _collect(app)
    started = asyncio.Event()

    @app.task()
    async def slow():
        started.set()
        await asyncio.sleep(30)

    run = asyncio.ensure_future(app._execute("t1", _pack(app, "slow"), 1))
    await asyncio.wait_for(started.wait(), timeout=5)
    app._running["t1"].cancel()
    outcome, _, _ = await asyncio.wait_for(run, timeout=5)

    assert outcome == 1
    assert seen == []


async def test_hook_fires_through_a_real_worker(redis, make_app):
    app = make_app("err_e2e", poll_block_ms=50, burst=True)
    seen = _collect(app)

    @app.task(max_retries=0)
    def boom():
        raise RuntimeError("from the worker")

    job = await boom.enqueue()
    await asyncio.wait_for(app.run(), timeout=15)

    result = await job.result()
    assert result is not None and not result.success
    assert len(seen) == 1
    assert seen[0].task_id == job.id and isinstance(seen[0].exc, RuntimeError)
