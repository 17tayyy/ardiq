"""job.result(timeout=) blocks until the result is stored, or raises TimeoutError."""

import asyncio
import time

import pytest


async def test_result_wait_blocks_until_done(redis, make_app):
    app = make_app("resultwait", concurrency=2, poll_block_ms=50)

    @app.task()
    async def slow():
        await asyncio.sleep(0.3)
        return 7

    job = await slow.enqueue()
    run = asyncio.ensure_future(app.run())
    try:
        res = await job.result(timeout=5)
    finally:
        app.stop()
        await asyncio.wait_for(run, timeout=5)

    assert res is not None and res.success and res.value == 7


async def test_result_wait_times_out(redis, make_app):
    app = make_app("resultwait2")

    @app.task()
    async def never():
        return 1

    job = await never.enqueue()  # no worker running → never completes
    with pytest.raises(TimeoutError):
        await job.result(timeout=0.3)


async def test_result_returns_immediately_when_already_stored(redis, make_app, poll):
    """The immediate path: when the result is already in Redis, result(timeout=)
    returns it without waiting for a signal."""
    app = make_app("resultdone", concurrency=2, poll_block_ms=50)

    @app.task()
    async def fast():
        return 99

    job = await fast.enqueue()
    run = asyncio.ensure_future(app.run())
    try:

        async def done():
            return await job.status() == "complete"

        assert await poll(done), "task never completed"
        res = await job.result(timeout=5)  # already stored → no signal needed
    finally:
        app.stop()
        await asyncio.wait_for(run, timeout=5)

    assert res is not None and res.success and res.value == 99


async def test_result_wait_wakes_promptly_on_signal(redis, make_app):
    """A waiter blocked before completion is pushed the result via pub/sub: it
    wakes within ms of the task finishing, not after the timeout's fallback GET."""
    app = make_app("resultprompt", concurrency=2, poll_block_ms=50)

    @app.task()
    async def quick():
        await asyncio.sleep(0.2)
        return 42

    job = await quick.enqueue()
    run = asyncio.ensure_future(app.run())
    try:
        res = await job.result(timeout=5)  # blocks on the signal path
        returned_ms = int(time.time() * 1000)
    finally:
        app.stop()
        await asyncio.wait_for(run, timeout=5)

    assert res is not None and res.value == 42
    # res.finish = when the worker stored the result. The signal should have woken
    # the waiter within ms; a broken PUBLISH would fall back to the 5s timeout.
    wakeup_ms = returned_ms - res.finish
    assert wakeup_ms < 1000, wakeup_ms
