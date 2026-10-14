"""Priority ordering, delayed/scheduled delivery, and burst draining due tasks."""

import asyncio
import time

import pytest


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


async def test_no_priority_lands_in_the_middle_lane(redis, make_app):
    """Forgetting `priority=` must not quietly demote the task to the bottom."""
    app = make_app("prio_default", priorities=["low", "default", "high"])

    @app.task()
    def whatever(): ...

    assert app.default_priority == "default"
    await whatever.enqueue()

    assert await redis.xlen("ardiq:prio_default:queues:low") == 0
    assert await redis.xlen("ardiq:prio_default:queues:default") == 1


async def test_default_priority_can_be_chosen(redis, make_app):
    app = make_app("prio_chosen", priorities=["low", "high"], default_priority="low")

    @app.task()
    def whatever(): ...

    await whatever.enqueue()

    assert await redis.xlen("ardiq:prio_chosen:queues:low") == 1
    assert await redis.xlen("ardiq:prio_chosen:queues:high") == 0


async def test_default_priority_must_be_a_real_lane(make_app):
    with pytest.raises(ValueError, match="not one of"):
        make_app("prio_bad", priorities=["low", "high"], default_priority="urgent")


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


async def test_waking_up_holds_no_more_than_prefetch(redis, make_app, poll):
    # An idle worker blocks on every lane at once, and Redis applies COUNT to
    # each stream: a batch landing in three lanes used to hand it three tasks.
    lanes = ["low", "default", "high"]
    app = make_app(
        "wake", priorities=lanes, concurrency=1, prefetch=1, poll_block_ms=1000
    )
    started = asyncio.Event()
    finish = asyncio.Event()
    order: list[str] = []

    @app.task()
    async def hold(lane):
        order.append(lane)
        started.set()
        await finish.wait()

    run = asyncio.ensure_future(app.run())
    try:
        await asyncio.sleep(0.3)
        jobs = await app.enqueue_many(
            hold.options(priority=lane).prepare(lane) for lane in lanes
        )
        await asyncio.wait_for(started.wait(), timeout=5)

        held = [
            (await redis.xpending(f"ardiq:wake:queues:{lane}", "workers"))["pending"]
            for lane in lanes
        ]
        assert sum(held) == 1

        finish.set()
        for job in jobs:
            assert await poll(lambda job=job: _complete(app, job.id))
        assert order == ["high", "default", "low"]
    finally:
        finish.set()
        app.stop()
        await asyncio.wait_for(run, timeout=15)


def test_prefetch_defaults_to_concurrency_with_several_lanes(make_app):
    assert make_app("pf", priorities=["low", "high"], concurrency=3).prefetch == 3
    assert make_app("pf", concurrency=3).prefetch == 6
    assert make_app("pf", priorities=["low", "high"], prefetch=5).prefetch == 5


async def test_a_later_higher_lane_task_does_not_wait_behind_prefetched_work(
    redis, make_app, poll
):
    # Issue #2: with prefetch above concurrency, the next low task was already in
    # memory when the high one arrived, so high waited for two low tasks.
    app = make_app("overtake", priorities=["low", "high"], concurrency=1)
    started = asyncio.Event()
    finish = asyncio.Event()
    order: list[str] = []

    @app.task()
    async def hold(tag):
        order.append(tag)
        started.set()
        await finish.wait()

    jobs = [await hold.options(priority="low").enqueue(f"low{i}") for i in range(2)]
    run = asyncio.ensure_future(app.run())
    try:
        await asyncio.wait_for(started.wait(), timeout=5)
        jobs.append(await hold.options(priority="high").enqueue("high"))
        finish.set()
        for job in jobs:
            assert await poll(lambda job=job: _complete(app, job.id))
        assert order == ["low0", "high", "low1"]
    finally:
        finish.set()
        app.stop()
        await asyncio.wait_for(run, timeout=15)
