"""A crashed worker's in-flight message is reclaimed via XAUTOCLAIM."""

import asyncio


async def test_reclaim_orphan(redis, make_app):
    app = make_app(
        "reclaim", idle_timeout_ms=200, poll_block_ms=100, burst=True, concurrency=2
    )
    stream = "ardiq:reclaim:queues:default"
    group = "workers"

    @app.task()
    def recovered():
        return 99

    j = await recovered.enqueue()

    # Simulate a worker that read the message then died: leave it in the PEL
    # under a consumer that never acks. XAUTOCLAIM in the next live worker
    # should pick it up once it goes idle past idle_timeout_ms.
    await redis.xgroup_create(stream, group, id="0", mkstream=True)
    delivered = await redis.xreadgroup(group, "dead-worker", {stream: ">"}, count=1)
    assert delivered

    await asyncio.sleep(0.3)  # age past idle_timeout_ms
    await asyncio.wait_for(app.run(), timeout=15)

    res = await j.result()
    assert res is not None and res.success and res.value == 99
