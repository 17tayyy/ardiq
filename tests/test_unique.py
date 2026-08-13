"""Unique tasks: an identical call already in flight is the same task."""

import asyncio


async def test_duplicate_enqueue_returns_the_live_job(redis, make_app):
    app = make_app("uniq", burst=True, poll_block_ms=100)
    ran: list[int] = []

    @app.task(unique=True)
    async def charge(order_id: int):
        ran.append(order_id)
        return order_id

    first = await charge.enqueue(42)
    second = await charge.enqueue(42)

    assert second.id == first.id
    assert await app.queue_size() == 1

    await asyncio.wait_for(app.run(), timeout=30)
    assert ran == [42]
    assert (await second.result()).value == 42


async def test_different_arguments_are_different_tasks(redis, make_app):
    app = make_app("uniqargs", burst=True, concurrency=2, poll_block_ms=100)
    ran: list[int] = []

    @app.task(unique=True)
    async def charge(order_id: int):
        ran.append(order_id)

    first = await charge.enqueue(1)
    second = await charge.enqueue(2)

    assert first.id != second.id
    await asyncio.wait_for(app.run(), timeout=30)
    assert sorted(ran) == [1, 2]


async def test_keyword_order_does_not_change_the_identity(redis, make_app):
    app = make_app("uniqkw", burst=True, poll_block_ms=100)

    @app.task(unique=True)
    async def notify(to: str, subject: str):
        return f"{to}/{subject}"

    first = await notify.enqueue(to="ada", subject="hi")
    second = await notify.enqueue(subject="hi", to="ada")

    assert first.id == second.id
    assert await app.queue_size() == 1
    await asyncio.wait_for(app.run(), timeout=30)


async def test_the_id_is_the_same_in_another_process(make_app):
    producer = make_app("uniqid")
    worker = make_app("uniqid")

    assert producer._task_id("sync", (1,), {"full": True}, True) == worker._task_id(
        "sync", (1,), {"full": True}, True
    )
    assert producer._task_id("sync", (1,), {}, False) != worker._task_id(
        "sync", (1,), {}, False
    )


async def test_a_finished_unique_task_runs_again_with_no_stale_result(redis, make_app):
    runs: list[str] = []

    def worker():
        # A burst worker cancels itself on the way out, so the second run needs
        # its own app — same queue, same task.
        app = make_app("uniqagain", burst=True, poll_block_ms=100)

        @app.task(unique=True)
        async def sync_inbox(user: str):
            runs.append(user)
            return len(runs)

        return app, sync_inbox

    first, sync_inbox = worker()
    job = await sync_inbox.enqueue("ada")
    await asyncio.wait_for(first.run(), timeout=30)
    assert (await job.result()).value == 1

    second, sync_inbox = worker()
    again = await sync_inbox.enqueue("ada")
    assert again.id == job.id
    # The previous run's result must not answer for this one: it hasn't run yet.
    assert await again.result() is None
    assert await again.status() == "queued"

    await asyncio.wait_for(second.run(), timeout=30)
    assert runs == ["ada", "ada"]
    assert (await again.result()).value == 2


async def test_a_batch_collapses_identical_unique_tasks(redis, make_app):
    app = make_app("uniqbatch", burst=True, concurrency=4, poll_block_ms=100)
    ran: list[str] = []

    @app.task(unique=True)
    async def rebuild(shop: str):
        ran.append(shop)

    jobs = await app.enqueue_many(
        [rebuild.prepare("a"), rebuild.prepare("a"), rebuild.prepare("b")]
    )

    assert jobs[0].id == jobs[1].id
    assert jobs[2].id != jobs[0].id
    assert await app.queue_size() == 2

    await asyncio.wait_for(app.run(), timeout=30)
    assert sorted(ran) == ["a", "b"]


async def test_a_batch_collapses_onto_a_task_already_queued(redis, make_app):
    app = make_app("uniqbatch2", burst=True, concurrency=4, poll_block_ms=100)
    ran: list[str] = []

    @app.task(unique=True)
    async def rebuild(shop: str):
        ran.append(shop)

    await rebuild.enqueue("a")
    await app.enqueue_many([rebuild.prepare("a")])

    assert await app.queue_size() == 1
    await asyncio.wait_for(app.run(), timeout=30)
    assert ran == ["a"]


async def test_ref_can_dedup_without_the_registry(redis, make_app):
    app = make_app("uniqref", burst=True, poll_block_ms=100)
    ran: list[str] = []

    @app.task()
    async def report(day: str):
        ran.append(day)

    handle = app.ref("report", unique=True)
    first = await handle.enqueue("2026-08-13")
    second = await handle.enqueue("2026-08-13")

    assert first.id == second.id
    await asyncio.wait_for(app.run(), timeout=30)
    assert ran == ["2026-08-13"]


async def test_options_turns_uniqueness_on_and_off(redis, make_app):
    app = make_app("uniqopts", burst=True, poll_block_ms=100)

    @app.task(unique=True)
    async def charge(order_id: int):
        return order_id

    @app.task()
    async def email(to: str):
        return to

    # off for a task that declared it
    loose = await charge.options(unique=False).enqueue(7)
    other = await charge.options(unique=False).enqueue(7)
    assert loose.id != other.id

    # on for one that didn't
    first = await email.options(unique=True).enqueue("ada")
    second = await email.options(unique=True).enqueue("ada")
    assert first.id == second.id

    await asyncio.wait_for(app.run(), timeout=30)


async def test_uniqueness_holds_while_the_task_waits_to_be_due(redis, make_app):
    app = make_app("uniqdelay", priorities=["default"])

    @app.task(unique=True)
    async def later(x: int):
        return x

    first = await later.options(delay_ms=5000).enqueue(1)
    second = await later.options(delay_ms=5000).enqueue(1)

    assert first.id == second.id
    assert await redis.zcard("ardiq:uniqdelay:queues:delayed:default") == 1
    assert await first.status() == "scheduled"


async def test_a_plain_task_still_gets_a_fresh_id_every_time(redis, make_app):
    app = make_app("uniqoff", burst=True, concurrency=2, poll_block_ms=100)
    ran: list[int] = []

    @app.task()
    async def charge(order_id: int):
        ran.append(order_id)

    first = await charge.enqueue(1)
    second = await charge.enqueue(1)

    assert first.id != second.id
    await asyncio.wait_for(app.run(), timeout=30)
    assert ran == [1, 1]


async def _scheduled(job) -> bool:
    return await job.status() == "scheduled"


async def test_a_retrying_unique_task_keeps_its_slot(redis, make_app, poll):
    app = make_app("uniqretry", concurrency=1, poll_block_ms=50)
    attempts: list[int] = []

    @app.task(unique=True, max_retries=3, backoff_ms=30_000)
    async def flaky(x: int):
        attempts.append(x)
        raise RuntimeError("not yet")

    worker = asyncio.ensure_future(app.run())
    job = await flaky.enqueue(1)
    assert await poll(lambda: _scheduled(job)), "the retry was never staged"

    # It is waiting out its backoff, which still counts as in flight.
    again = await flaky.enqueue(1)
    assert again.id == job.id
    assert await redis.zcard("ardiq:uniqretry:queues:delayed:default") == 1
    assert attempts == [1]

    app.stop()
    await asyncio.wait_for(worker, timeout=15)


async def test_aborting_a_unique_task_frees_the_call(redis, make_app):
    app = make_app("uniqabort", poll_block_ms=50)

    @app.task(unique=True)
    async def sync_shop(shop: str):
        return shop

    first = await sync_shop.options(delay_ms=60_000).enqueue("ada")
    assert await first.abort() is True
    assert (await first.result()).aborted

    second = await sync_shop.options(delay_ms=60_000).enqueue("ada")

    assert second.id == first.id
    assert await second.status() == "scheduled"  # not the aborted result
    assert await second.result() is None
