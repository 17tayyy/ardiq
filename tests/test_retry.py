"""Terminal failure (outcome 1) and the retry bounce through the delayed ZSET."""

import asyncio
import time

import msgpack
import pytest

from ardiq import RETRY, SUCCESS, execute, pack_task, register, unpack_result


def _boom() -> int:
    raise ValueError("boom")


async def test_terminal_failure(redis, make_core):
    register("boom", _boom)  # default max_retries=0 → terminal
    core = make_core("retry_fail", burst=True, concurrency=2, poll_block_ms=100)
    await core.enqueue("fail-1", pack_task("boom"))
    await asyncio.wait_for(core.run(execute), timeout=15)

    res = unpack_result(await core.result("fail-1"))
    assert res is not None and res.success is False and "boom" in str(res.value)
    cleaned = await redis.exists(
        "ardiq:retry_fail:task:data:fail-1", "ardiq:retry_fail:task:retry:fail-1"
    )
    assert cleaned == 0
    assert not await redis.sismember("ardiq:retry_fail:index:running", "fail-1")


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
            return RETRY, b"", retry_after_ms
        return SUCCESS, msgpack.packb({"attempts": len(tries_seen)}), 0

    core = make_core("retry_run", concurrency=2, poll_block_ms=50)
    await core.enqueue("rt-1", pack_task("noop"))

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
