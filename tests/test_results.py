"""result/status across the lifecycle: not_found → queued → complete."""

import asyncio

from ardiq import execute, pack_task, register, unpack_result


def _double(x: int) -> int:
    return x * 2


def _explode() -> int:
    raise RuntimeError("nope")


async def test_lifecycle(redis, make_core):
    register("double", _double)
    register("explode", _explode)  # max_retries=0 → terminal
    core = make_core("results", burst=True, concurrency=2, poll_block_ms=100)

    assert await core.status("ghost") == "not_found"
    assert await core.result("ghost") is None

    await core.enqueue("ok-1", pack_task("double", (21,)))
    await core.enqueue("bad-1", pack_task("explode"))
    assert await core.status("ok-1") == "queued"

    await asyncio.wait_for(core.run(execute), timeout=15)

    assert await core.status("ok-1") == "complete"
    res = unpack_result(await core.result("ok-1"))
    assert res is not None and res.success and res.value == 42

    assert await core.status("bad-1") == "complete"  # finished, not succeeded
    res = unpack_result(await core.result("bad-1"))
    assert res is not None and res.success is False and "nope" in str(res.value)
