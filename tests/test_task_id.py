"""Custom task ids: one live task per id, and a reused id is a fresh run."""

import asyncio


async def test_an_id_already_queued_is_the_same_task(redis, make_app):
    app = make_app("taskid", burst=True, poll_block_ms=100)
    ran: list[int] = []

    @app.task
    async def sync_account(n: int):
        ran.append(n)

    first = await sync_account.options(task_id="sync:1").enqueue(1)
    second = await sync_account.options(task_id="sync:1").enqueue(2)

    assert second.id == first.id
    assert await app.queue_size() == 1
    await asyncio.wait_for(app.run(), timeout=30)
    assert ran == [1]


async def test_a_finished_id_runs_again_with_no_stale_result(redis, make_app):
    app = make_app("taskidagain", burst=True, poll_block_ms=100)
    runs: list[str] = []

    @app.task
    async def sync_account(user: str):
        runs.append(user)
        return len(runs)

    job = await sync_account.options(task_id="sync:ada").enqueue("ada")
    await asyncio.wait_for(app.run(), timeout=30)
    assert (await job.result()).value == 1

    again = await sync_account.options(task_id="sync:ada").enqueue("ada")
    assert again.id == job.id
    assert await again.result() is None
    assert await again.status() == "queued"

    await asyncio.wait_for(app.run(), timeout=30)
    assert runs == ["ada", "ada"]
    assert (await again.result()).value == 2


async def test_a_batch_reusing_a_finished_id_drops_its_result(redis, make_app):
    app = make_app("taskidbatch", burst=True, poll_block_ms=100)

    @app.task
    async def sync_account(user: str):
        return user

    await sync_account.options(task_id="sync:ada").enqueue("ada")
    await asyncio.wait_for(app.run(), timeout=30)

    [again] = await app.enqueue_many(
        [sync_account.options(task_id="sync:ada").prepare("ada")]
    )
    assert await again.result() is None
    assert await again.status() == "queued"
