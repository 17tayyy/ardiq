"""Batch enqueue: `Task.prepare` plus `Ardiq.enqueue_many`."""

import asyncio

import pytest


async def test_batch_runs_every_task(redis, make_app):
    app = make_app("batch", burst=True, concurrency=4, poll_block_ms=100)
    seen: list[int] = []

    @app.task()
    async def job(n: int):
        seen.append(n)
        return n

    jobs = await app.enqueue_many(job.prepare(i) for i in range(50))
    await asyncio.wait_for(app.run(), timeout=30)

    assert len(jobs) == 50
    assert sorted(seen) == list(range(50))
    results = [await j.result() for j in jobs]
    assert [r.value for r in results] == list(range(50))


async def test_batch_mixes_tasks_and_options(redis, make_app):
    app = make_app("batchmix", burst=True, priorities=["low", "high"], concurrency=4)
    ran: list[str] = []

    @app.task()
    async def charge(order_id: int):
        ran.append(f"charge:{order_id}")

    @app.task()
    async def email(to: str):
        ran.append(f"email:{to}")

    await app.enqueue_many(
        [
            charge.prepare(1),
            email.prepare("a@b.c"),
            charge.options(priority="high").prepare(2),
        ]
    )
    await asyncio.wait_for(app.run(), timeout=30)

    assert sorted(ran) == ["charge:1", "charge:2", "email:a@b.c"]


async def test_batch_keeps_order_and_ids(redis, make_app):
    app = make_app("batchids")

    @app.task()
    async def job(n: int):
        return n

    jobs = await app.enqueue_many(
        [job.options(task_id="first").prepare(1), job.prepare(2)]
    )

    assert jobs[0].id == "first"
    assert jobs[1].id != "first"
    assert [await j.status() for j in jobs] == ["queued", "queued"]


async def test_batch_delay_lands_in_the_delayed_set(redis, make_app):
    app = make_app("batchdelay")

    @app.task()
    async def job(n: int):
        return n

    await app.enqueue_many([job.options(delay_ms=60_000).prepare(1)])

    # Held in the delayed set, not readable from the stream yet.
    assert await redis.zcard("ardiq:batchdelay:queues:delayed:default") == 1
    assert await redis.xlen("ardiq:batchdelay:queues:default") == 0


async def test_bad_lane_sends_nothing(redis, make_app):
    # Validated across the whole batch first: no half-enqueued batches.
    app = make_app("batchlane", priorities=["low", "high"])

    @app.task()
    async def job(n: int):
        return n

    with pytest.raises(ValueError, match="urgent"):
        await app.enqueue_many(
            [job.prepare(1), job.options(priority="urgent").prepare(2)]
        )

    assert await app.queue_size() == 0


async def test_empty_batch_is_a_no_op(redis, make_app):
    app = make_app("batchempty")
    assert await app.enqueue_many([]) == []
