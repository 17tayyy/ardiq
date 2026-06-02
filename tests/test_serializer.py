"""A pluggable serializer (e.g. pickle) lets tasks pass types msgpack cannot."""

import asyncio
import pickle
from datetime import datetime


async def test_pickle_round_trips_datetime(redis, make_app):
    app = make_app(
        "pickle",
        concurrency=2,
        poll_block_ms=50,
        burst=True,
        serializer=pickle.dumps,
        deserializer=pickle.loads,
    )

    @app.task()
    async def echo(value):
        return value

    moment = datetime(2026, 1, 2, 3, 4, 5)
    job = await echo.enqueue(moment)
    await asyncio.wait_for(app.run(), timeout=15)

    res = await job.result()
    assert res is not None and res.success and res.value == moment
