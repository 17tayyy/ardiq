"""Terminal failure (outcome 1) and the retry bounce through the delayed ZSET."""

import asyncio
import time

import msgpack
import pytest

from ardiq import ArdiqCore


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
RETRY, SUCCESS = 1, 0


@pytest.mark.parametrize(
    ("retry_after_ms", "min_gap", "max_gap"),
    [(0, 0.9, 2.5), (50, 0.0, 0.6)],  # default tries²·1000 backoff vs explicit
)
async def test_retry_mechanism(redis, make_core, poll, retry_after_ms, min_gap, max_gap):
    tries_seen: list[int] = []
    stamps: list[float] = []

    async def executor(task_id, payload, tries):
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
