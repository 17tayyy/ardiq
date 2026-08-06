"""`current_task()`: a task reading its own id from inside the body."""

import asyncio

from ardiq import current_task


def _pack(app, fn_name):
    return app._dumps({"f": fn_name, "a": [], "k": {}, "t": 0})


def test_there_is_no_current_task_outside_a_worker():
    assert current_task() is None


async def test_an_async_task_sees_itself(make_app):
    app = make_app("ctx_async")
    seen = []

    @app.task()
    async def work():
        seen.append(current_task())

    await app._execute("job-1", _pack(app, "work"), 1)

    assert len(seen) == 1
    ctx = seen[0]
    assert ctx is not None
    assert ctx.task_id == "job-1" and ctx.name == "work" and ctx.tries == 1


async def test_a_sync_task_sees_itself_from_its_thread(make_app):
    """ContextVars ride along into asyncio.to_thread, so sync tasks get it too."""
    app = make_app("ctx_sync")
    seen = []

    @app.task()
    def work():
        seen.append(current_task())

    await app._execute("job-2", _pack(app, "work"), 1)

    assert seen[0] is not None and seen[0].task_id == "job-2"


async def test_it_reports_the_current_attempt(make_app):
    app = make_app("ctx_tries")
    seen = []

    @app.task(max_retries=2, backoff_ms=1)
    async def flaky():
        seen.append(current_task())
        raise ValueError("again")

    await app._execute("job-3", _pack(app, "flaky"), 1)
    await app._execute("job-3", _pack(app, "flaky"), 2)

    assert [ctx.tries for ctx in seen] == [1, 2]


async def test_it_is_cleared_when_the_task_ends(make_app):
    app = make_app("ctx_cleared")

    @app.task(max_retries=0)
    async def boom():
        raise ValueError("x")

    await app._execute("job-4", _pack(app, "boom"), 1)

    assert current_task() is None


async def test_concurrent_tasks_do_not_see_each_other(make_app):
    app = make_app("ctx_concurrent")
    seen = {}

    @app.task()
    async def work(): ...

    @app.task(name="slow")
    async def slow():
        await asyncio.sleep(0.05)
        seen["slow"] = current_task()

    @app.task(name="quick")
    async def quick():
        seen["quick"] = current_task()

    await asyncio.gather(
        app._execute("slow-id", _pack(app, "slow"), 1),
        app._execute("quick-id", _pack(app, "quick"), 1),
    )

    assert seen["slow"].task_id == "slow-id"
    assert seen["quick"].task_id == "quick-id"
