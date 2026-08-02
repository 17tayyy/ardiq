"""Worker lifespan: startup/shutdown hooks and the resources they put on app.state."""

import asyncio

import pytest


async def test_lifespan_runs_around_the_worker(redis, make_app):
    app = make_app("lifespan_basic", poll_block_ms=50, burst=True)
    events = []

    @app.lifespan
    async def lifespan():
        events.append("setup")
        yield {"db": "pool"}
        events.append("teardown")

    @app.task()
    async def work():
        events.append(f"task:{app.state.db}")
        return "ok"

    job = await work.enqueue()
    await asyncio.wait_for(app.run(), timeout=15)

    assert events == ["setup", "task:pool", "teardown"]
    result = await job.result()
    assert result is not None and result.success


async def test_lifespan_can_set_state_directly(redis, make_app):
    """Yielding nothing is fine — assign to app.state yourself."""
    app = make_app("lifespan_direct", poll_block_ms=50, burst=True)
    closed = []

    @app.lifespan
    async def lifespan():
        app.state.client = "http"
        yield
        closed.append(app.state.client)

    @app.task()
    async def work():
        return app.state.client

    job = await work.enqueue()
    await asyncio.wait_for(app.run(), timeout=15)

    result = await job.result()
    assert result is not None and result.value == "http"
    assert closed == ["http"]


async def test_lifespan_available_to_sync_tasks(redis, make_app):
    app = make_app("lifespan_sync", poll_block_ms=50, burst=True)

    @app.lifespan
    async def lifespan():
        yield {"factor": 3}

    @app.task()
    def multiply(x):
        return x * app.state.factor

    job = await multiply.enqueue(7)
    await asyncio.wait_for(app.run(), timeout=15)

    result = await job.result()
    assert result is not None and result.value == 21


async def test_lifespan_tears_down_on_failure(make_app):
    """A crashing loop still unwinds the lifespan."""
    app = make_app("lifespan_teardown")
    events = []

    @app.lifespan
    async def lifespan():
        events.append("setup")
        try:
            yield
        finally:
            events.append("teardown")

    with pytest.raises(RuntimeError, match="loop died"):
        async with app._lifespan_scope():
            raise RuntimeError("loop died")

    assert events == ["setup", "teardown"]


async def test_lifespan_setup_error_propagates(redis, make_app):
    app = make_app("lifespan_boom", poll_block_ms=50, burst=True)

    @app.lifespan
    async def lifespan():
        raise RuntimeError("no database")
        yield

    with pytest.raises(RuntimeError, match="no database"):
        await asyncio.wait_for(app.run(), timeout=15)


async def test_lifespan_rejects_a_plain_coroutine(make_app):
    app = make_app("lifespan_bad")

    with pytest.raises(TypeError, match="async generator"):

        @app.lifespan
        async def lifespan():
            return None


async def test_lifespan_rejects_a_non_mapping_yield(redis, make_app):
    app = make_app("lifespan_nonmap", poll_block_ms=50, burst=True)

    @app.lifespan
    async def lifespan():
        yield ["not", "a", "mapping"]

    with pytest.raises(TypeError, match="mapping"):
        await asyncio.wait_for(app.run(), timeout=15)


async def test_state_error_names_the_missing_key(make_app):
    app = make_app("lifespan_state")

    with pytest.raises(AttributeError, match="lifespan"):
        _ = app.state.db


async def test_no_lifespan_is_fine(redis, make_app):
    app = make_app("lifespan_none", poll_block_ms=50, burst=True)

    @app.task()
    async def work():
        return "ok"

    job = await work.enqueue()
    await asyncio.wait_for(app.run(), timeout=15)

    result = await job.result()
    assert result is not None and result.success
