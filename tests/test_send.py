"""Enqueuing by name: `app.send` and `app.ref`, with no local registration."""

import asyncio

import pytest


async def test_send_reaches_a_worker_that_owns_the_task(redis, make_app):
    """The point of `send`: the producer never imports the task module."""
    worker = make_app("send_e2e", poll_block_ms=50, burst=True)
    producer = make_app("send_e2e")

    @worker.task()
    def add(a, b):
        return a + b

    job = await producer.send("add", 2, b=3)
    assert producer.tasks == []  # nothing registered on this side

    await asyncio.wait_for(worker.run(), timeout=15)

    result = await job.result()
    assert result is not None and result.success and result.value == 5


async def test_send_with_an_unknown_name_fails_on_the_worker(redis, make_app):
    app = make_app("send_unknown", poll_block_ms=50, burst=True)

    job = await app.send("nope")
    await asyncio.wait_for(app.run(), timeout=15)

    result = await job.result()
    assert result is not None and not result.success
    assert result.value == "unknown task 'nope'"


async def test_ref_carries_enqueue_options(redis, make_app):
    app = make_app("send_options")

    job = await app.ref("later").options(task_id="fixed", delay_ms=60_000).enqueue(1)

    assert job.id == "fixed"
    assert await job.status() == "scheduled"
    info = await job.info()
    assert info is not None and info.fn_name == "later" and info.args == (1,)


async def test_ref_priority_is_honored(redis, make_app):
    app = make_app(
        "send_prio",
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
        await app.ref("record", priority="low").enqueue(f"low{i}")
        await app.ref("record").options(priority="high").enqueue(f"high{i}")

    await asyncio.wait_for(app.run(), timeout=15)

    assert all(t.startswith("high") for t in order[:3])
    assert all(t.startswith("low") for t in order[3:])


async def test_options_still_defaults_to_the_tasks_own_priority(redis, make_app):
    """`.options()` without a priority must not drop the one from `@task`."""
    app = make_app(
        "send_prio_default",
        priorities=["low", "high"],
        concurrency=1,
        prefetch=10,
        poll_block_ms=50,
        burst=True,
    )
    order: list[str] = []

    @app.task(priority="high")
    def urgent(tag):
        order.append(tag)

    @app.task(priority="low")
    def whenever(tag):
        order.append(tag)

    await whenever.options(task_id="w").enqueue("low")
    await urgent.options(task_id="u").enqueue("high")

    await asyncio.wait_for(app.run(), timeout=15)

    assert order == ["high", "low"]


async def test_a_ref_cannot_be_called_locally(make_app):
    app = make_app("send_call")

    with pytest.raises(TypeError, match="reference"):
        app.ref("elsewhere")()
