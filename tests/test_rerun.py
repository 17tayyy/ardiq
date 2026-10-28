"""Running a worker again: an app serves its queue after a burst drain or a stop."""

import asyncio


async def test_a_burst_app_runs_again(redis, make_app):
    app = make_app("rerun_burst", burst=True, poll_block_ms=50)
    ran: list[int] = []

    @app.task
    async def probe(n: int):
        ran.append(n)

    await probe.enqueue(1)
    await asyncio.wait_for(app.run(), timeout=30)
    await probe.enqueue(2)
    await asyncio.wait_for(app.run(), timeout=30)

    assert ran == [1, 2]
    assert await app.queue_size() == 0


async def test_an_app_runs_again_after_a_stop(redis, make_app, poll):
    app = make_app("rerun_stop", poll_block_ms=50)
    ran: list[int] = []

    @app.task
    async def probe(n: int):
        ran.append(n)

    for n in (1, 2):

        async def done(n: int = n) -> bool:
            return n in ran

        await probe.enqueue(n)
        run = asyncio.ensure_future(app.run())
        try:
            assert await poll(done)
        finally:
            app.stop()
            await asyncio.wait_for(run, timeout=5)

    assert ran == [1, 2]


async def test_a_stop_before_the_run_starts_ends_only_that_run(redis, make_app):
    app = make_app("rerun_early_stop", poll_block_ms=50)
    ran: list[int] = []

    @app.task
    async def probe(n: int):
        ran.append(n)

    run = asyncio.ensure_future(app.run())
    app.stop()
    await asyncio.wait_for(run, timeout=5)

    await probe.enqueue(1)
    app.burst = True
    await asyncio.wait_for(app.run(), timeout=30)
    assert ran == [1]
