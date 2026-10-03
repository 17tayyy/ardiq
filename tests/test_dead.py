"""The dead letter queue: tasks that fail for good are kept for replay."""

import asyncio

import pytest

from ardiq import Ardiq, DeadLetter
from ardiq.testing import inline


async def _drain(app):
    await asyncio.wait_for(app.run(), timeout=30)


async def _drain_again(make_app, queue, *tasks):
    """A burst worker runs once, so a second drain needs a fresh one."""
    worker = make_app(queue, burst=True, poll_block_ms=100)
    for task in tasks:
        worker.task(max_retries=0, name=task.name)(task.fn)
    await _drain(worker)


async def test_a_task_out_of_retries_lands_in_the_dlq(redis, make_app):
    app = make_app("dead_basic", burst=True, poll_block_ms=100)

    @app.task(max_retries=1, backoff_ms=1)
    async def charge(order_id: int, *, amount: int) -> None:
        raise ValueError("card declined")

    job = await charge.enqueue(7, amount=300)
    await _drain(app)

    assert await app.dead_count() == 1
    [dead] = await app.dead_letters()
    assert isinstance(dead, DeadLetter)
    assert dead.task_id == job.id
    assert dead.fn_name == "charge"
    assert dead.args == (7,) and dead.kwargs == {"amount": 300}
    assert dead.priority == app.default_priority
    assert "card declined" in dead.error
    assert dead.tries == 2
    assert dead.failed_at >= dead.enqueue_time > 0


async def test_the_entry_outlives_the_result(redis, make_app):
    app = make_app("dead_ttl", burst=True, poll_block_ms=100, result_ttl_ms=50)

    @app.task(max_retries=0)
    async def boom() -> None:
        raise RuntimeError("x")

    job = await boom.enqueue()
    await _drain(app)
    await asyncio.sleep(0.2)

    assert await job.result() is None
    assert await app.dead_count() == 1


async def test_success_retry_and_abort_never_land_there(redis, make_app):
    app = make_app("dead_none", burst=True, poll_block_ms=100)
    tries = 0

    @app.task
    async def ok() -> int:
        return 1

    @app.task(max_retries=2, backoff_ms=1)
    async def flaky() -> None:
        nonlocal tries
        tries += 1
        if tries < 2:
            raise ValueError("once")

    @app.task
    async def doomed() -> None:
        pass

    await ok.enqueue()
    await flaky.enqueue()
    job = await doomed.options(delay_ms=60_000).enqueue()
    assert await job.abort()
    await _drain(app)

    assert await app.dead_count() == 0
    assert await app.dead_letters() == []


async def test_unknown_tasks_land_there(redis, make_app):
    app = make_app("dead_unknown", burst=True, poll_block_ms=100)

    job = await app.send("not_deployed_yet", 1)
    await _drain(app)

    [dead] = await app.dead_letters()
    assert dead.task_id == job.id and "unknown task" in dead.error


async def test_replay_reruns_with_the_same_id(redis, make_app):
    app = make_app("dead_replay", burst=True, poll_block_ms=100)
    broken = True

    @app.task(max_retries=0)
    async def sync_crm(user_id: int) -> int:
        if broken:
            raise ConnectionError("crm down")
        return user_id

    job = await sync_crm.enqueue(5)
    await _drain(app)
    assert await app.dead_count() == 1

    broken = False
    replayed = await app.replay(job.id)
    assert replayed is not None and replayed.id == job.id
    # The old failure is gone before the rerun, not reported as its outcome.
    assert await replayed.status() == "queued"
    assert await app.dead_count() == 0

    await _drain_again(make_app, "dead_replay", sync_crm)
    result = await replayed.result()
    assert result is not None and result.success and result.value == 5
    assert result.tries == 1


async def test_a_replay_that_dies_again_is_dead_again(redis, make_app):
    app = make_app("dead_again", burst=True, poll_block_ms=100)

    @app.task(max_retries=0)
    async def boom() -> None:
        raise RuntimeError("still broken")

    job = await boom.enqueue()
    await _drain(app)
    await app.replay(job.id)
    await _drain_again(make_app, "dead_again", boom)

    [dead] = await app.dead_letters()
    assert dead.task_id == job.id


async def test_replay_and_delete_of_an_unknown_id(redis, make_app):
    app = make_app("dead_missing")

    assert await app.replay("nope") is None
    assert await app.delete_dead("nope") is False


async def test_delete_drops_without_running(redis, make_app):
    app = make_app("dead_delete", burst=True, poll_block_ms=100)

    @app.task(max_retries=0)
    async def boom() -> None:
        raise RuntimeError("x")

    job = await boom.enqueue()
    await _drain(app)

    assert await app.delete_dead(job.id) is True
    assert await app.dead_count() == 0
    assert await app.replay(job.id) is None


async def test_newest_first_and_limit(redis, make_app):
    app = make_app("dead_order", burst=True, poll_block_ms=100, concurrency=1)

    @app.task(max_retries=0)
    async def boom(n: int) -> None:
        await asyncio.sleep(0.01)  # distinct failure times
        raise RuntimeError(str(n))

    for n in range(3):
        await boom.enqueue(n)
    await _drain(app)

    assert [d.args for d in await app.dead_letters()] == [(2,), (1,), (0,)]
    assert [d.args for d in await app.dead_letters(limit=1)] == [(2,)]
    assert await app.dead_letters(limit=0) == []


async def test_replay_refuses_a_lane_no_longer_configured(redis, make_app):
    old = make_app("dead_lane", priorities=["low", "high"], burst=True)

    @old.task(max_retries=0, priority="high")
    async def boom() -> None:
        raise RuntimeError("x")

    job = await boom.enqueue()
    await _drain(old)

    new = make_app("dead_lane", priorities=["low"])
    with pytest.raises(ValueError, match="high"):
        await new.replay(job.id)
    assert await new.dead_count() == 1


async def test_inline_mode_keeps_a_dlq_too():
    app = Ardiq(redis_url="redis://localhost:1")
    broken = True

    @app.task(max_retries=0)
    async def boom() -> str:
        if broken:
            raise RuntimeError("x")
        return "fixed"

    async with inline(app):
        job = await boom.enqueue()
        [dead] = await app.dead_letters()
        assert dead.task_id == job.id

        broken = False
        replayed = await app.replay(job.id)
        assert replayed is not None
        result = await replayed.result()
        assert result is not None and result.value == "fixed"
        assert await app.dead_count() == 0
