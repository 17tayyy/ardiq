"""Blocking sync tasks run in threads, so they don't serialize the event loop."""

import asyncio
import time


async def test_sync_tasks_run_in_parallel(redis, make_app):
    app = make_app("sync", concurrency=4, prefetch=8, poll_block_ms=50, burst=True)

    @app.task()
    def blocking(n: int) -> int:
        time.sleep(0.3)  # releases the GIL → real parallelism across threads
        return n

    jobs = [await blocking.enqueue(i) for i in range(4)]

    start = time.monotonic()
    await asyncio.wait_for(app.run(), timeout=15)
    elapsed = time.monotonic() - start

    for job in jobs:
        res = await job.result()
        assert res is not None and res.success

    # Threaded: ~0.3s total. Inline (loop-blocking): ~1.2s. The gap is the point.
    assert elapsed < 0.8, f"sync tasks serialized ({elapsed:.2f}s) — not threaded"
