"""Terminal failure (outcome 1) and the retry bounce through the delayed ZSET."""

import asyncio
import logging
import time

import msgpack
import pytest

from ardiq import ArdiqCore, Retry


async def test_terminal_failure(redis, make_app):
    app = make_app("retry_fail", burst=True, concurrency=2, poll_block_ms=100)

    @app.task(max_retries=0)
    def boom():
        raise ValueError("boom")

    j = await boom.enqueue()
    await asyncio.wait_for(app.run(), timeout=15)

    res = await j.result()
    assert res is not None and res.success is False and "boom" in str(res.value)

    task_id = j.id
    cleaned = await redis.exists(
        f"ardiq:retry_fail:task:data:{task_id}",
        f"ardiq:retry_fail:task:retry:{task_id}",
    )
    assert cleaned == 0
    assert not await redis.sismember("ardiq:retry_fail:index:running", task_id)


# These two cases test the raw core retry mechanism with a custom executor
# (outcome 2 directly), so they still use ArdiqCore directly.


@pytest.mark.parametrize(
    ("retry_after_ms", "min_gap", "max_gap"),
    [(0, 0.9, 2.5), (50, 0.0, 0.6)],  # default tries²·1000 backoff vs explicit
)
async def test_retry_mechanism(
    redis, make_core, poll, retry_after_ms, min_gap, max_gap
):
    tries_seen: list[int] = []
    stamps: list[float] = []

    async def executor(task_id, payload, tries, aborted):
        stamps.append(time.monotonic())
        tries_seen.append(tries)
        if len(tries_seen) < 2:
            return 2, b"", retry_after_ms  # outcome 2 = RETRY
        return 0, msgpack.packb({"attempts": len(tries_seen)}), 0  # outcome 0 = SUCCESS

    core: ArdiqCore = make_core("retry_run", concurrency=2, poll_block_ms=50)
    payload = msgpack.packb({"f": "noop", "a": [], "k": {}, "t": 0})
    await core.enqueue("rt-1", payload)

    run = asyncio.ensure_future(core.run(executor))  # pyo3 future, not a coroutine
    try:
        assert await poll(lambda: core.result("rt-1"))
    finally:
        core.stop()
        await asyncio.wait_for(run, timeout=5)

    assert tries_seen == [1, 2]  # redelivery proves the delayed bounce
    env = msgpack.unpackb(await core.result("rt-1"), raw=False)
    assert env["attempts"] == 2
    assert min_gap <= stamps[1] - stamps[0] <= max_gap


# `Retry` raised by the task itself: same bounce, chosen delay, quieter.


def _pack(app, fn_name):
    return app._dumps({"f": fn_name, "a": [], "k": {}, "t": 0})


async def test_manual_retry_picks_its_own_delay(make_app):
    app = make_app("retry_manual")

    @app.task(max_retries=3, backoff_ms=5000)
    async def rate_limited():
        raise Retry("slow down", delay_ms=250)

    outcome, _, retry_ms = await app._execute("t1", _pack(app, "rate_limited"), 1)

    assert outcome == 2  # RETRY
    assert retry_ms == 250  # the task's delay beats the task's backoff_ms


async def test_manual_retry_falls_back_to_the_configured_backoff(make_app):
    app = make_app("retry_manual_default")

    @app.task(max_retries=3, backoff_ms=5000)
    async def again():
        raise Retry()

    outcome, _, retry_ms = await app._execute("t1", _pack(app, "again"), 1)

    assert outcome == 2
    assert retry_ms == 5000


async def test_manual_retry_respects_max_retries(make_app):
    app = make_app("retry_manual_budget")
    seen = []

    @app.on_error
    def record(ctx):
        seen.append(ctx)

    @app.task(max_retries=1, backoff_ms=1)
    async def forever():
        raise Retry("still not ready")

    first, _, _ = await app._execute("t1", _pack(app, "forever"), 1)
    last, env, _ = await app._execute("t1", _pack(app, "forever"), 2)

    assert first == 2  # RETRY
    assert last == 1  # FAILURE — the budget is not infinite
    result = app._unpack(env)
    assert result is not None and "still not ready" in str(result.value)

    # Asked-for retries stay out of the hooks; giving up does not.
    assert len(seen) == 1
    assert isinstance(seen[0].exc, Retry) and seen[0].will_retry is False


async def test_manual_retry_logs_at_info(make_app, caplog):
    app = make_app("retry_manual_log")

    @app.task(max_retries=1, backoff_ms=1)
    async def again():
        raise Retry("waiting on upstream")

    with caplog.at_level(logging.INFO, logger="ardiq"):
        await app._execute("t1", _pack(app, "again"), 1)

    records = [r for r in caplog.records if r.message.startswith("task retry")]
    assert len(records) == 1
    assert records[0].levelno == logging.INFO  # not a warning: this was on purpose


async def test_manual_retry_end_to_end(redis, make_app, poll):
    app = make_app("retry_manual_e2e", concurrency=2, poll_block_ms=50)
    attempts: list[int] = []

    @app.task(max_retries=3)
    async def flaky():
        attempts.append(1)
        if len(attempts) < 2:
            raise Retry("not yet", delay_ms=50)
        return "ok"

    job = await flaky.enqueue()
    run = asyncio.ensure_future(app.run())
    try:
        assert await poll(lambda: _complete(app, job.id))
    finally:
        app.stop()
        await asyncio.wait_for(run, timeout=15)

    result = await job.result()
    assert result is not None and result.success and result.value == "ok"
    assert result.tries == 2


async def _complete(app, task_id: str) -> bool:
    return await app.status(task_id) == "complete"
