"""TaskResult carries enqueue/start/finish timings."""

import asyncio


async def test_result_has_timings(redis, make_app):
    app = make_app("timings", concurrency=2, poll_block_ms=50, burst=True)

    @app.task()
    async def work():
        await asyncio.sleep(0.1)
        return "ok"

    job = await work.enqueue()
    await asyncio.wait_for(app.run(), timeout=15)

    res = await job.result()
    assert res is not None and res.success
    assert res.enqueue_time > 0
    assert res.start >= res.enqueue_time
    assert res.finish >= res.start
    assert res.duration_ms >= 90
