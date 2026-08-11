"""Abort/cancel: stopping a task that is queued, delayed, or already running."""

import asyncio
import logging

import pytest


def _pack(app, fn_name, args=(), kwargs=None):
    return app._dumps({"f": fn_name, "a": list(args), "k": kwargs or {}, "t": 0})


async def test_aborted_at_pickup_skips_the_task(make_app, caplog):
    app = make_app("abort_pickup")
    ran = []

    @app.task()
    async def work():
        ran.append(1)

    with caplog.at_level(logging.INFO, logger="ardiq"):
        outcome, env, _ = await app._execute("t1", _pack(app, "work"), 1, True)

    assert outcome == 1  # FAILURE
    assert not ran  # never invoked
    result = app._unpack(env)
    assert result is not None and result.aborted and not result.success
    assert any(r.message.startswith("task aborted") for r in caplog.records)


async def test_aborted_at_pickup_wins_over_unknown_task(make_app):
    """An abort should not surface as a spurious 'unknown task' error."""
    app = make_app("abort_unknown")

    outcome, env, _ = await app._execute("t2", _pack(app, "gone"), 1, True)

    assert outcome == 1
    result = app._unpack(env)
    assert result is not None and result.aborted


async def test_cancelling_in_flight_task_reports_aborted(make_app, caplog):
    app = make_app("abort_inflight")
    started = asyncio.Event()

    @app.task()
    async def slow():
        started.set()
        await asyncio.sleep(30)

    with caplog.at_level(logging.WARNING, logger="ardiq"):
        run = asyncio.ensure_future(app._execute("t3", _pack(app, "slow"), 1))
        await asyncio.wait_for(started.wait(), timeout=5)
        assert "t3" in app._running  # registered while in flight
        app._running["t3"].cancel()
        outcome, env, _ = await asyncio.wait_for(run, timeout=5)

    assert outcome == 1
    result = app._unpack(env)
    assert result is not None and result.aborted
    assert any(r.message.startswith("task aborted") for r in caplog.records)
    assert "t3" not in app._running  # deregistered on the way out


async def test_successful_task_is_deregistered(make_app):
    app = make_app("abort_registry")

    @app.task()
    async def quick():
        return "ok"

    outcome, _, _ = await app._execute("t4", _pack(app, "quick"), 1)

    assert outcome == 0  # SUCCESS
    assert app._running == {}


async def test_abort_delayed_task_finalizes_immediately(redis, make_app):
    """A task still waiting on its delay is ours to cancel outright."""
    app = make_app("abort_delayed")

    @app.task()
    async def later():
        return "should not run"

    job = await later.options(delay_ms=60_000).enqueue()
    assert await job.status() == "scheduled"

    assert await job.abort() is True

    result = await job.result()
    assert result is not None and result.aborted
    assert await job.status() == "complete"
    assert await app.queue_size() == 0


async def test_abort_queued_task_is_honored_at_pickup(redis, make_app):
    """A task already in the live stream can only be dropped by the worker."""
    app = make_app("abort_queued", poll_block_ms=50, burst=True)
    ran = []

    @app.task()
    async def work():
        ran.append(1)
        return "ran"

    job = await work.enqueue()
    assert await job.abort() is True

    await asyncio.wait_for(app.run(), timeout=15)

    assert not ran
    result = await job.result()
    assert result is not None and result.aborted


async def test_abort_returns_false_when_already_finished(redis, make_app):
    app = make_app("abort_done", poll_block_ms=50, burst=True)

    @app.task()
    async def work():
        return "done"

    job = await work.enqueue()
    await asyncio.wait_for(app.run(), timeout=15)
    result = await job.result()
    assert result is not None and result.success

    assert await job.abort() is False


async def test_abort_returns_false_for_unknown_task(redis, make_app):
    app = make_app("abort_missing")
    assert await app.abort("never-existed") is False


async def test_abort_running_task_cancels_it(redis, make_app, poll):
    """The end-to-end path: publish on the abort channel, worker cancels."""
    app = make_app("abort_running", concurrency=2, poll_block_ms=50)
    finished = []

    @app.task(max_retries=0)
    async def slow():
        await asyncio.sleep(30)
        finished.append(1)
        return "done"

    worker = asyncio.ensure_future(app.run())
    job = await slow.enqueue()
    assert await poll(lambda: _is_running(job)), "task never started"

    assert await job.abort() is True

    result = await asyncio.wait_for(job.result(timeout=10), timeout=15)
    assert result is not None and result.aborted
    assert not finished

    app.stop()
    await asyncio.wait_for(worker, timeout=15)


async def _is_running(job) -> bool:
    return await job.status() == "running"


@pytest.mark.parametrize("burst", [True, False])
async def test_abort_is_idempotent(redis, make_app, burst):
    app = make_app("abort_twice", poll_block_ms=50, burst=burst)

    @app.task()
    async def work():
        return "ran"

    job = await work.options(delay_ms=60_000).enqueue()
    assert await job.abort() is True
    assert await job.abort() is False  # already finalized
