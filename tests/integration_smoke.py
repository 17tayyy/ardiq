"""Minimal end-to-end check of the Rust core <-> Python bridge.

Enqueue an async job that returns 67, run the worker in burst mode (it processes
the queue then exits), and assert the result landed in Redis. A green run proves
the asyncio bridge actually executes Python tasks driven from the Rust loop.

Run: `uv run python tests/integration_smoke.py` (needs Redis on localhost:6379).
"""

import asyncio
import sys

import msgpack
import redis.asyncio as aioredis

from ardiq import ArdiqCore, execute, pack_task, register

# Dedicated logical DB so the test never touches real data.
REDIS_URL = "redis://localhost:6379/15"
QUEUE = "smoke"
TASK_ID = "job-1"
RESULT_KEY = f"ardiq:{QUEUE}:task:results:{TASK_ID}"


async def job() -> int:
    return 67


async def main() -> int:
    redis = aioredis.from_url(REDIS_URL)
    await redis.flushdb()

    register("job", job)
    core = ArdiqCore(
        {
            "redis_url": REDIS_URL,
            "queue_name": QUEUE,
            "burst": True,
            "concurrency": 4,
            "poll_block_ms": 200,
        }
    )

    queued = await core.enqueue(TASK_ID, pack_task("job"))
    assert queued is True, "enqueue reported the task as a duplicate"

    try:
        await asyncio.wait_for(core.run(execute), timeout=15)
    except TimeoutError:
        core.stop()
        print("FAIL: worker did not finish — the asyncio bridge likely hung")
        return 1

    raw = await redis.get(RESULT_KEY)
    if raw is None:
        print(f"FAIL: no result stored at {RESULT_KEY}")
        return 1

    envelope = msgpack.unpackb(raw, raw=False)
    if not (envelope.get("s") is True and envelope.get("r") == 67):
        print(f"FAIL: unexpected result envelope: {envelope}")
        return 1

    print(f"PASS: worker {core.worker_id} ran job -> {envelope['r']}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
