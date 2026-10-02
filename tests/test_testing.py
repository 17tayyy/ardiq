"""`ardiq.testing.inline`: tasks run on enqueue, with no Redis behind them."""

import asyncio

import pytest

from ardiq import Ardiq, Job, Retry, TaskResult, current_task
from ardiq.testing import inline

# Nothing listens here: any call that reached Redis would fail.
NO_REDIS = "redis://localhost:1"


def _app(**kw) -> Ardiq:
    return Ardiq(redis_url=NO_REDIS, **kw)


async def _result(job: Job) -> TaskResult:
    result = await job.result()
    assert result is not None
    return result


async def test_enqueue_runs_the_task_and_stores_its_result():
    app = _app()

    @app.task
    async def add(a: int, b: int) -> int:
        return a + b

    async with inline(app):
        job = await add.enqueue(2, 3)
        result = await job.result()
        assert result is not None and result.success and result.value == 5
        assert await job.status() == "complete"
        assert await job.info() is None
        assert await app.queue_size() == 0


async def test_result_with_timeout_returns_at_once():
    app = _app()

    @app.task
    async def ping() -> str:
        return "pong"

    async with inline(app):
        job = await ping.enqueue()
        result = await job.result(timeout=5)
        assert result is not None and result.value == "pong"
        with pytest.raises(TimeoutError):
            await app.result("never-enqueued", timeout=5)


async def test_sync_tasks_run_in_a_thread():
    app = _app()

    @app.task
    def double(x: int) -> int:
        return x * 2

    async with inline(app):
        assert (await _result(await double.enqueue(21))).value == 42


async def test_failures_retry_back_to_back_then_fail():
    app = _app()
    seen = []
    app.on_error(seen.append)
    calls = 0

    @app.task(max_retries=2, backoff_ms=60_000)
    async def flaky() -> None:
        nonlocal calls
        calls += 1
        raise ValueError("nope")

    async with inline(app):
        result = await _result(await flaky.enqueue())

    assert calls == 3
    assert not result.success and result.tries == 3
    assert "ValueError" in result.value
    assert [c.will_retry for c in seen] == [True, True, False]


async def test_retry_succeeds_on_a_later_try():
    app = _app()

    @app.task(max_retries=3)
    async def eventually() -> int:
        task = current_task()
        assert task is not None
        if task.tries < 3:
            raise Retry("not yet", delay_ms=60_000)
        return task.tries

    async with inline(app):
        result = await _result(await eventually.enqueue())
        assert result.success and result.value == 3


async def test_timeouts_apply():
    app = _app()

    @app.task(timeout=0.05, max_retries=0)
    async def slow() -> None:
        await asyncio.sleep(10)

    async with inline(app):
        result = await _result(await slow.enqueue())
        assert not result.success and "timed out" in result.value


async def test_unknown_task_fails_like_on_a_worker():
    app = _app()

    async with inline(app):
        result = await _result(await app.send("missing"))
        assert not result.success and "unknown task" in result.value


async def test_args_go_through_the_serializer():
    app = _app()

    @app.task
    async def echo(x: object) -> object:
        return x

    async with inline(app):
        # msgpack has no tuples: what a worker would see is what a test sees.
        assert (await _result(await echo.enqueue((1, 2)))).value == [1, 2]
        with pytest.raises(TypeError):
            await echo.enqueue(object())


async def test_lifespan_runs_around_the_block():
    app = _app()
    events = []

    @app.lifespan
    async def setup():
        events.append("start")
        yield {"db": "fake-db"}
        events.append("stop")

    @app.task
    async def use_db() -> str:
        return app.state.db

    async with inline(app):
        assert events == ["start"]
        assert (await _result(await use_db.enqueue())).value == "fake-db"
    assert events == ["start", "stop"]


async def test_options_and_enqueue_many():
    app = _app(priorities=["low", "high"])

    @app.task
    async def square(x: int) -> int:
        return x * x

    async with inline(app):
        job = await square.options(task_id="sq", delay_ms=60_000).enqueue(4)
        assert job.id == "sq" and (await _result(job)).value == 16
        jobs = await app.enqueue_many(square.prepare(n) for n in range(3))
        assert [(await _result(j)).value for j in jobs] == [0, 1, 4]
        with pytest.raises(ValueError):
            await square.options(priority="urgent").enqueue(1)


async def test_tasks_can_enqueue_tasks():
    app = _app()

    @app.task
    async def child(x: int) -> int:
        return x + 1

    @app.task
    async def parent(x: int) -> int:
        job = await child.enqueue(x)
        return (await _result(job)).value

    async with inline(app):
        assert (await _result(await parent.enqueue(1))).value == 2


async def test_unique_skips_a_call_already_running():
    app = _app()
    runs = 0

    @app.task(unique=True)
    async def once(x: int) -> None:
        nonlocal runs
        runs += 1
        if runs == 1:
            await once.enqueue(x)  # same call, still in flight

    async with inline(app):
        await once.enqueue(1)
        assert runs == 1
        await once.enqueue(1)  # finished, so it runs again
        assert runs == 2


async def test_abort_cancels_a_running_task():
    app = _app()
    started = asyncio.Event()

    @app.task
    async def hang() -> None:
        started.set()
        await asyncio.sleep(10)

    async with inline(app):
        enqueue = asyncio.ensure_future(hang.options(task_id="h").enqueue())
        await started.wait()
        assert await app.status("h") == "running"
        info = await app.info("h")
        assert info is not None and info.fn_name == "hang" and info.tries == 1
        assert await app.abort("h") is True
        job = await enqueue
        assert (await _result(job)).aborted
        assert await job.abort() is False  # already finished


async def test_core_is_restored_and_run_is_refused():
    app = _app()
    real = app._core

    async with inline(app):
        with pytest.raises(RuntimeError, match="inline"):
            await app.run()
    assert app._core is real
