"""Public API: @task, the enqueue client, and auto-retry up to max_retries."""

import asyncio


async def _complete(app, task_id: str) -> bool:
    return await app.status(task_id) == "complete"


async def test_public_api(redis, make_app, poll):
    app = make_app("api", concurrency=2, poll_block_ms=50)
    flaky_calls: list[int] = []

    @app.task()
    def add(a, b):
        return a + b

    @app.task(max_retries=3, backoff_ms=50)
    def flaky():
        flaky_calls.append(1)
        if len(flaky_calls) < 3:
            raise ValueError("not yet")
        return "ok"

    @app.task(max_retries=0)
    def boom():
        raise RuntimeError("dead")

    add_job = await add.enqueue(2, 3)
    flaky_job = await flaky.enqueue()
    boom_job = await boom.options(task_id="boomer").enqueue()

    run = asyncio.ensure_future(app.run())
    try:
        done = await asyncio.gather(
            poll(lambda: _complete(app, add_job.id)),
            poll(lambda: _complete(app, flaky_job.id)),
            poll(lambda: _complete(app, boom_job.id)),
        )
    finally:
        app.stop()
        await asyncio.wait_for(run, timeout=5)
    assert all(done)

    res = await add_job.result()
    assert res is not None and res.success and res.value == 5

    res = await flaky_job.result()
    assert res is not None and res.success and res.value == "ok"
    assert len(flaky_calls) == 3  # one initial + two retries

    assert boom_job.id == "boomer"
    res = await boom_job.result()
    assert res is not None and res.success is False and "dead" in str(res.value)
