"""Shared fixtures. All tests need Redis on localhost:6379 (skipped if absent)."""

import asyncio
import time

import pytest
import redis.asyncio as aioredis

from ardiq import REGISTRY, Ardiq, ArdiqCore

REDIS_URL = "redis://localhost:6379/15"  # isolated DB


@pytest.fixture
async def redis():
    client = aioredis.from_url(REDIS_URL)
    try:
        await client.ping()
    except Exception:
        pytest.skip("Redis not available on localhost:6379")
    await client.flushdb()
    REGISTRY.clear()
    yield client
    await client.aclose()


@pytest.fixture
def make_core():
    def _make(queue: str, **kw):
        return ArdiqCore({"redis_url": REDIS_URL, "queue_name": queue, **kw})

    return _make


@pytest.fixture
def make_app():
    def _make(queue: str, **kw):
        return Ardiq(redis_url=REDIS_URL, queue_name=queue, **kw)

    return _make


@pytest.fixture
def poll():
    async def _poll(predicate, timeout: float = 10.0, interval: float = 0.02) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if await predicate():
                return True
            await asyncio.sleep(interval)
        return False

    return _poll
