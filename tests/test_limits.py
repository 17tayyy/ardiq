"""Lane limits: a lane capped at N never has more than N tasks in a worker,
running or prefetched, while the other lanes use the rest of `concurrency`."""

import asyncio

import pytest


def test_limits_are_read_back_lowest_first(make_app):
    app = make_app(
        "lim-read",
        priorities=["heavy", "default", "high"],
        limits={"high": 2, "heavy": 1},
    )

    assert list(app.limits.items()) == [("heavy", 1), ("high", 2)]
    assert make_app("lim-none").limits == {}


@pytest.mark.parametrize(
    ("limits", "message"),
    [
        ({"nope": 1}, 'limits names lane "nope"'),
        ({"heavy": 0}, 'limit for lane "heavy" must be positive'),
    ],
)
def test_bad_limits_are_refused(make_app, limits, message):
    with pytest.raises(ValueError, match=message):
        make_app("lim-bad", priorities=["heavy", "default"], limits=limits)


@pytest.mark.parametrize("weights", [None, {"heavy": 1, "default": 1}])
async def test_a_limited_lane_runs_one_at_a_time_beside_the_others(
    redis, make_app, poll, weights
):
    # Issue #1: one heavy task at a time, while the light ones keep running.
    queue = f"lim-heavy-{bool(weights)}"
    app = make_app(
        queue,
        priorities=["heavy", "default"],
        limits={"heavy": 1},
        weights=weights,
        concurrency=4,
        poll_block_ms=50,
    )
    release = asyncio.Event()
    running = 0
    peak = 0

    @app.task(priority="heavy")
    async def scene(n):
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        try:
            await release.wait()
        finally:
            running -= 1

    @app.task(priority="default")
    async def health(n):
        pass

    jobs = [await scene.enqueue(n) for n in range(3)]
    run = asyncio.ensure_future(app.run())
    try:

        async def one_scene_running():
            return running == 1

        assert await poll(one_scene_running)
        light = [await health.enqueue(n) for n in range(5)]
        for job in light:
            assert (await job.result(timeout=5)).success
        # The other scenes were never read, so no other worker is kept from them.
        pending = await redis.xpending(f"ardiq:{queue}:queues:heavy", "workers")
        assert pending["pending"] == 1

        release.set()
        for job in jobs:
            assert (await job.result(timeout=5)).success
    finally:
        release.set()
        app.stop()
        await asyncio.wait_for(run, timeout=15)
    assert peak == 1


async def test_burst_drains_a_limited_lane(redis, make_app):
    app = make_app(
        "lim-burst",
        priorities=["heavy", "default"],
        limits={"heavy": 1},
        concurrency=4,
        poll_block_ms=50,
        burst=True,
    )
    done: list[str] = []

    @app.task()
    async def work(lane):
        await asyncio.sleep(0.01)
        done.append(lane)

    for lane in ("heavy", "default"):
        for _ in range(5):
            await work.options(priority=lane).enqueue(lane)

    await asyncio.wait_for(app.run(), timeout=15)

    assert sorted(done) == ["default"] * 5 + ["heavy"] * 5
