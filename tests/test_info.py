"""job.info() metadata and the "scheduled" vs "queued" status distinction."""


async def test_scheduled_status_and_info(redis, make_app):
    app = make_app("info", concurrency=2, poll_block_ms=50)

    @app.task()
    async def work(x):
        return x

    job = await work.options(delay_ms=60_000).enqueue(42, foo="bar")
    assert await job.status() == "scheduled"

    info = await job.info()
    assert info is not None
    assert info.fn_name == "work"
    assert info.args == (42,)
    assert info.kwargs == {"foo": "bar"}
    assert info.status == "scheduled"
    assert info.tries == 0
    assert info.scheduled_at and info.scheduled_at > 0


async def test_queued_info_and_unknown(redis, make_app):
    app = make_app("info2", concurrency=2, poll_block_ms=50)

    @app.task()
    async def work(x):
        return x

    job = await work.enqueue(7)
    assert await job.status() == "queued"
    info = await job.info()
    assert info is not None and info.status == "queued" and info.scheduled_at is None

    assert await app.info("ghost") is None
