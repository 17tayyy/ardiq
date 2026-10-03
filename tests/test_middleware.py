"""`@app.middleware` around each attempt, and `@app.on_enqueue` headers."""

import asyncio
import contextvars

import pytest

from ardiq import Ardiq, EnqueueContext, ExecutionContext, Job, Retry, TaskResult
from ardiq.testing import inline

NO_REDIS = "redis://localhost:1"


def _app(**kw) -> Ardiq:
    return Ardiq(redis_url=NO_REDIS, **kw)


async def _result(job: Job) -> TaskResult:
    result = await job.result()
    assert result is not None
    return result


async def test_first_registered_is_outermost():
    app = _app()
    order = []

    @app.middleware
    async def outer(ctx, call_next):
        order.append("outer in")
        result = await call_next()
        order.append("outer out")
        return result

    @app.middleware
    async def inner(ctx, call_next):
        order.append("inner in")
        result = await call_next()
        order.append("inner out")
        return result

    @app.task
    async def work() -> str:
        order.append("task")
        return "done"

    async with inline(app):
        assert (await _result(await work.enqueue())).value == "done"
    assert order == ["outer in", "inner in", "task", "inner out", "outer out"]


async def test_middleware_sees_the_call_and_can_change_the_result():
    app = _app()
    seen: list[ExecutionContext] = []

    @app.middleware
    async def double(ctx, call_next):
        seen.append(ctx)
        return await call_next() * 2

    @app.task
    async def add(a: int, b: int = 0) -> int:
        return a + b

    async with inline(app):
        job = await add.enqueue(2, b=3)
        assert (await _result(job)).value == 10

    [ctx] = seen
    assert isinstance(ctx, ExecutionContext)
    assert (ctx.task_id, ctx.name, ctx.tries) == (job.id, "add", 1)
    assert ctx.args == (2,) and ctx.kwargs == {"b": 3} and ctx.headers == {}


async def test_failures_pass_through_and_retries_still_apply():
    app = _app()
    seen = []

    @app.middleware
    async def watch(ctx, call_next):
        try:
            return await call_next()
        except Exception as exc:
            seen.append((ctx.tries, type(exc).__name__))
            raise

    @app.task(max_retries=2)
    async def flaky() -> str:
        raise Retry("not yet")

    async with inline(app):
        result = await _result(await flaky.enqueue())

    assert not result.success and result.tries == 3
    assert seen == [(1, "Retry"), (2, "Retry"), (3, "Retry")]


async def test_a_raising_middleware_fails_the_attempt():
    app = _app()

    @app.middleware
    async def deny(ctx, call_next):
        raise PermissionError("tenant suspended")

    @app.task(max_retries=0)
    async def work() -> None:
        raise AssertionError("never runs")

    async with inline(app):
        result = await _result(await work.enqueue())
        assert not result.success and "tenant suspended" in result.value
        assert await app.dead_count() == 1


async def test_the_timeout_covers_the_task_alone():
    app = _app()
    seen = []

    @app.middleware
    async def watch(ctx, call_next):
        try:
            return await call_next()
        except TimeoutError:
            seen.append("timeout")
            raise

    @app.task(timeout=0.05, max_retries=0)
    async def slow() -> None:
        await asyncio.sleep(10)

    async with inline(app):
        result = await _result(await slow.enqueue())
    assert seen == ["timeout"] and "timed out" in result.value


async def test_context_set_by_middleware_reaches_a_sync_task():
    app = _app()
    tenant: contextvars.ContextVar[str] = contextvars.ContextVar("tenant")

    @app.middleware
    async def set_tenant(ctx, call_next):
        token = tenant.set(ctx.kwargs["tenant"])
        try:
            return await call_next()
        finally:
            tenant.reset(token)

    @app.task
    def whoami(*, tenant: str) -> str:
        return tenant_var()

    def tenant_var() -> str:
        return tenant.get()

    async with inline(app):
        assert (await _result(await whoami.enqueue(tenant="acme"))).value == "acme"


async def test_an_abort_reaches_the_middleware():
    app = _app()
    started = asyncio.Event()
    cleaned = []

    @app.middleware
    async def cleanup(ctx, call_next):
        try:
            return await call_next()
        finally:
            cleaned.append(ctx.task_id)

    @app.task
    async def hang() -> None:
        started.set()
        await asyncio.sleep(10)

    async with inline(app):
        run = asyncio.ensure_future(hang.options(task_id="h").enqueue())
        await started.wait()
        await app.abort("h")
        assert (await _result(await run)).aborted
    assert cleaned == ["h"]


def test_middleware_must_be_async():
    app = _app()

    with pytest.raises(TypeError, match="async"):

        @app.middleware
        def sync_mw(ctx, call_next):
            return call_next()

    class Timing:
        async def __call__(self, ctx, call_next):
            return await call_next()

    timing = Timing()
    assert app.middleware(timing) is timing


async def test_headers_travel_from_enqueue_to_middleware():
    app = _app()
    enqueued: list[EnqueueContext] = []
    received = []

    @app.on_enqueue
    def trace(ctx):
        enqueued.append(ctx)
        ctx.headers["traceparent"] = f"trace-{ctx.name}"

    @app.on_enqueue
    async def tenant(ctx):
        ctx.headers["tenant"] = "acme"

    @app.middleware
    async def read(ctx, call_next):
        received.append(ctx.headers)
        return await call_next()

    @app.task
    async def work(n: int) -> int:
        return n

    async with inline(app):
        job = await work.enqueue(1)
        await app.send("work", 2)
        await app.enqueue_many([work.prepare(3)])

    assert enqueued[0].task_id == job.id and enqueued[0].args == (1,)
    assert received == [{"traceparent": "trace-work", "tenant": "acme"}] * 3


async def test_a_raising_enqueue_hook_sends_nothing():
    app = _app()
    ran = []

    @app.on_enqueue
    def broken(ctx):
        raise RuntimeError("no trace context")

    @app.task
    async def work() -> None:
        ran.append(1)

    async with inline(app):
        with pytest.raises(RuntimeError, match="no trace context"):
            await work.enqueue()
    assert ran == []


async def test_no_hooks_means_no_headers_in_the_payload():
    app = _app()
    assert "h" not in app._loads(app._pack("work", (), {}))
    assert "h" not in app._loads(app._pack("work", (), {}, {}))


async def test_replay_keeps_the_original_headers():
    app = _app()
    received = []
    broken = True

    @app.on_enqueue
    def stamp(ctx):
        ctx.headers["n"] = len(received)

    @app.middleware
    async def read(ctx, call_next):
        received.append(ctx.headers["n"])
        return await call_next()

    @app.task(max_retries=0)
    async def work() -> None:
        if broken:
            raise RuntimeError("x")

    async with inline(app):
        job = await work.enqueue()
        broken = False
        await app.replay(job.id)
    assert received == [0, 0]


async def test_headers_cross_redis_and_old_payloads_still_run(redis, make_app):
    app = make_app("mw_redis", burst=True, poll_block_ms=50)
    received = []

    @app.on_enqueue
    def stamp(ctx):
        ctx.headers["request_id"] = "r-1"

    @app.middleware
    async def read(ctx, call_next):
        received.append(ctx.headers)
        return await call_next()

    @app.task
    async def work() -> None:
        pass

    await work.enqueue()
    # What a pre-headers producer sends: no "h" at all.
    old = app._dumps({"f": "work", "a": [], "k": {}, "t": 0})
    await app._core.enqueue("old-1", old, None, 0, 0, 0, False)
    await asyncio.wait_for(app.run(), timeout=15)

    assert sorted(received, key=len) == [{}, {"request_id": "r-1"}]
