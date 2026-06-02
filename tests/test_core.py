"""Core success path: the Rust loop drives a Python task and stores its result."""

import asyncio


async def test_success(redis, make_app):
    app = make_app("smoke", burst=True, concurrency=4, poll_block_ms=200)

    @app.task()
    async def job():
        return 67

    job_handle = await job.enqueue()
    await asyncio.wait_for(app.run(), timeout=15)

    res = await job_handle.result()
    assert res is not None and res.success and res.value == 67


async def test_burst_with_blocking_poll(redis, make_app):
    # Regression: poll_block_ms >= 500 used to desync the producer connection
    # (redis's default 500ms response timeout vs XREADGROUP BLOCK) and hang burst.
    app = make_app("blockpoll", burst=True, concurrency=2, poll_block_ms=500)

    @app.task()
    async def job():
        return 67

    j = await job.enqueue()
    await asyncio.wait_for(app.run(), timeout=10)

    res = await j.result()
    assert res is not None and res.success and res.value == 67
