"""The core counts every attempt per task and lane, without touching the tasks."""

import asyncio

from ardiq import Retry, TaskMetrics


def _by_task(app) -> dict[tuple[str, str], TaskMetrics]:
    return {(m.fn_name, m.priority): m for m in app.metrics()}


def test_nothing_counted_before_a_worker_runs(make_app):
    assert make_app("metrics-empty").metrics() == []


async def test_outcomes_are_counted_per_task_and_lane(redis, make_app):
    app = make_app("metrics", priorities=["low", "high"], poll_block_ms=50, burst=True)
    tries: list[int] = []

    @app.task()
    async def ok():
        pass

    @app.task()
    async def flaky():
        tries.append(1)
        if len(tries) == 1:
            raise Retry(delay_ms=1)

    @app.task(max_retries=0)
    async def boom():
        raise ValueError("boom")

    @app.task()
    async def doomed():
        pass

    for _ in range(3):
        await ok.options(priority="high").enqueue()
    await ok.options(priority="low").enqueue()
    await flaky.options(priority="high").enqueue()
    await boom.options(priority="low").enqueue()
    assert await (await doomed.options(priority="low").enqueue()).abort()

    await asyncio.wait_for(app.run(), timeout=15)
    seen = _by_task(app)

    assert set(seen) == {
        ("ok", "high"),
        ("ok", "low"),
        ("flaky", "high"),
        ("boom", "low"),
        ("doomed", "low"),
    }
    counts = {
        key: (m.succeeded, m.retried, m.failed, m.aborted) for key, m in seen.items()
    }
    assert counts[("ok", "high")] == (3, 0, 0, 0)
    assert counts[("ok", "low")] == (1, 0, 0, 0)
    assert counts[("flaky", "high")] == (1, 1, 0, 0)
    assert counts[("boom", "low")] == (0, 0, 1, 0)
    assert counts[("doomed", "low")] == (0, 0, 0, 1)
    assert seen[("flaky", "high")].run.count == 2  # every attempt is timed


async def test_run_and_wait_are_timed(redis, make_app, poll):
    app = make_app("metrics-time", poll_block_ms=50)

    @app.task()
    async def nap():
        await asyncio.sleep(0.1)

    # Due in 300 ms: the wait counts from then, not from the enqueue.
    job = await nap.options(delay_ms=300).enqueue()
    run = asyncio.ensure_future(app.run())
    try:
        assert await poll(lambda: _done(app, job.id))
    finally:
        app.stop()
        await asyncio.wait_for(run, timeout=15)

    [m] = app.metrics()
    assert m.run.count == m.wait.count == 1
    assert 0.1 <= m.run.sum < 0.25
    assert dict(m.run.buckets)[0.05] == 0
    assert dict(m.run.buckets)[0.25] == 1
    assert m.wait.sum < 0.25
    counts = [n for _, n in m.wait.buckets]
    assert counts == sorted(counts) and counts[-1] == 1  # cumulative


async def _done(app, task_id: str) -> bool:
    return await app.status(task_id) == "complete"
