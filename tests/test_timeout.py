"""Per-task timeout: a task that overruns @task(timeout=) fails or retries."""

import asyncio


async def test_task_timeout(redis, make_app):
    app = make_app("timeout", concurrency=2, poll_block_ms=50, burst=True)

    @app.task(timeout=0.2, max_retries=0)
    async def slow():
        await asyncio.sleep(2)
        return "done"

    @app.task(timeout=5)
    async def fast():
        return "ok"

    slow_job = await slow.enqueue()
    fast_job = await fast.enqueue()
    await asyncio.wait_for(app.run(), timeout=15)

    slow_res = await slow_job.result()
    assert slow_res is not None
    assert slow_res.success is False and "timed out" in str(slow_res.value)

    fast_res = await fast_job.result()
    assert fast_res is not None and fast_res.success and fast_res.value == "ok"
