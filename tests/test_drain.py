"""Stopping a worker: finish what is running, hand back what never started."""

import asyncio


async def test_prefetched_tasks_go_back_to_the_queue(redis, make_app):
    app = make_app("drain_release", concurrency=1, prefetch=4, poll_block_ms=50)
    stream = "ardiq:drain_release:queues:default"

    @app.task
    async def slow(n: int) -> int:
        await asyncio.sleep(0.5)
        return n

    jobs = [await slow.enqueue(n) for n in range(4)]
    run = asyncio.ensure_future(app.run())
    await asyncio.sleep(0.2)
    app.stop()
    await asyncio.wait_for(run, timeout=15)

    # The running one finished; the three it only held are free right away.
    assert [await job.status() for job in jobs] == ["complete"] + ["queued"] * 3
    assert (await redis.xpending(stream, "workers"))["pending"] == 0
    assert await redis.xlen(stream) == 3

    worker = make_app("drain_release", burst=True, poll_block_ms=50)
    worker.task(name="slow")(slow.fn)
    await asyncio.wait_for(worker.run(), timeout=15)
    results = [await job.result() for job in jobs]
    assert [r.value for r in results if r is not None] == [0, 1, 2, 3]
    assert all(r is not None and r.tries == 1 for r in results)


async def test_a_draining_task_keeps_its_heartbeat(redis, make_app):
    # A heartbeat every ~180 ms; without it the task would sit idle for the
    # whole drain and another worker could reclaim it mid-run.
    app = make_app("drain_beat", concurrency=1, idle_timeout_ms=200, poll_block_ms=50)
    stream = "ardiq:drain_beat:queues:default"

    @app.task
    async def slow() -> None:
        await asyncio.sleep(1)

    job = await slow.enqueue()
    run = asyncio.ensure_future(app.run())
    await asyncio.sleep(0.1)
    app.stop()
    await asyncio.sleep(0.7)

    [entry] = await redis.xpending_range(stream, "workers", "-", "+", 10)
    assert entry["time_since_delivered"] < 200
    await asyncio.wait_for(run, timeout=15)
    result = await job.result()
    assert result is not None and result.success
