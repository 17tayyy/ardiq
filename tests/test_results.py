"""result/status across the lifecycle: not_found → queued → complete."""

import asyncio


async def test_lifecycle(redis, make_app):
    app = make_app("results", burst=True, concurrency=2, poll_block_ms=100)

    @app.task()
    def double(x: int) -> int:
        return x * 2

    @app.task(max_retries=0)
    def explode() -> int:
        raise RuntimeError("nope")

    assert await app.status("ghost") == "not_found"
    assert await app.result("ghost") is None

    ok = await double.enqueue(21)
    bad = await explode.enqueue()
    assert await ok.status() == "queued"

    await asyncio.wait_for(app.run(), timeout=15)

    assert await ok.status() == "complete"
    res = await ok.result()
    assert res is not None and res.success and res.value == 42

    assert await bad.status() == "complete"  # finished, not succeeded
    res = await bad.result()
    assert res is not None and res.success is False and "nope" in str(res.value)
