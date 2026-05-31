"""Core success path: the Rust loop drives a Python task and stores its result."""

import asyncio

from ardiq import execute, pack_task, register, unpack_result


async def _job() -> int:
    return 67


async def test_success(redis, make_core):
    register("job", _job)
    core = make_core("smoke", burst=True, concurrency=4, poll_block_ms=200)

    assert await core.enqueue("job-1", pack_task("job")) is True
    await asyncio.wait_for(core.run(execute), timeout=15)

    res = unpack_result(await core.result("job-1"))
    assert res is not None and res.success and res.value == 67
