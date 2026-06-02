"""job.result(timeout=) blocks until the result is stored, or raises TimeoutError."""

import asyncio

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
