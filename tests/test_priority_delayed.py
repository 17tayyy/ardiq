"""Priority ordering, delayed/scheduled delivery, and burst draining due tasks."""

import asyncio
import time


async def _complete(app, task_id: str) -> bool:
    return await app.status(task_id) == "complete"


async def test_priority_high_before_low(redis, make_app):
    # priorities are passed lowest-first; "high" is read before "low".
    app = make_app(
        "prio",
        priorities=["low", "high"],
        concurrency=1,
        prefetch=10,
        poll_block_ms=50,
        burst=True,
    )
    order: list[str] = []

    @app.task()
    def record(tag):
        order.append(tag)

    for i in range(3):
        await record.options(priority="low").enqueue(f"low{i}")
        await record.options(priority="high").enqueue(f"high{i}")

    await asyncio.wait_for(app.run(), timeout=15)

    assert len(order) == 6
    assert all(t.startswith("high") for t in order[:3])
    assert all(t.startswith("low") for t in order[3:])


async def test_delayed_and_scheduled(redis, make_app, poll):
    app = make_app("sched", concurrency=2, poll_block_ms=50)
    fired: dict[str, float] = {}

    @app.task()
    def stamp(tag):
        fired[tag] = time.monotonic()

    t0 = time.monotonic()
    now_ms = int(time.time() * 1000)
    d_job = await stamp.options(delay_ms=300).enqueue("delay")
    s_job = await stamp.options(schedule_ms=now_ms + 300).enqueue("sched")
    assert await d_job.status() == "scheduled"
    assert await s_job.status() == "scheduled"

    run = asyncio.ensure_future(app.run())
    try:
        await poll(lambda: _complete(app, d_job.id))
        await poll(lambda: _complete(app, s_job.id))
    finally:
        app.stop()
        await asyncio.wait_for(run, timeout=5)

    assert 0.27 <= fired["delay"] - t0 <= 1.5
    assert 0.27 <= fired["sched"] - t0 <= 1.5


async def test_burst_drains_due_task(redis, make_app):
    app = make_app("burstdue", concurrency=2, poll_block_ms=50, burst=True)
    ran: list[str] = []

    @app.task()
    def mark(tag):
        ran.append(tag)

    past_ms = int(time.time() * 1000) - 1  # already due, routed via the delayed ZSET
    job = await mark.options(schedule_ms=past_ms).enqueue("due")
    await asyncio.wait_for(app.run(), timeout=15)

    assert ran == ["due"]
    res = await job.result()
    assert res is not None and res.success
