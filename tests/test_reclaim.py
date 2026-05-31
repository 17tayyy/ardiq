"""A crashed worker's in-flight message is reclaimed via XAUTOCLAIM."""

import asyncio

from ardiq import execute, pack_task, register, unpack_result


def _recovered() -> int:
    return 99


async def test_reclaim_orphan(redis, make_core):
    queue, task_id = "reclaim", "orphan-1"
    stream = f"ardiq:{queue}:queues:default"
    group = "workers"
    register("recovered", _recovered)

    core = make_core(queue, idle_timeout_ms=200, poll_block_ms=100, burst=True)
    await core.enqueue(task_id, pack_task("recovered"))

    # A worker that read the message then died: it sits in the group's PEL under
    # a consumer that never acks.
    await redis.xgroup_create(stream, group, id="0", mkstream=True)
    delivered = await redis.xreadgroup(group, "dead-worker", {stream: ">"}, count=1)
    assert delivered

    await asyncio.sleep(0.3)  # age past idle_timeout_ms
    await asyncio.wait_for(core.run(execute), timeout=15)

    res = unpack_result(await core.result(task_id))
    assert res is not None and res.success and res.value == 99
