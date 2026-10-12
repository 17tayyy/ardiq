"""Weighted lanes: a busy higher lane no longer starves the lower ones."""

import asyncio

import pytest

from ardiq import Ardiq


async def _drain(app, lanes: dict[str, int]) -> list[str]:
    order: list[str] = []

    @app.task()
    def record(lane):
        order.append(lane)

    for lane, n in lanes.items():
        for _ in range(n):
            await record.options(priority=lane).enqueue(lane)

    await asyncio.wait_for(app.run(), timeout=15)
    return order


async def test_lanes_drain_strictly_without_weights(redis, make_app):
    app = make_app(
        "fair-strict",
        priorities=["low", "high"],
        concurrency=1,
        prefetch=1,
        poll_block_ms=50,
        burst=True,
    )

    order = await _drain(app, {"high": 12, "low": 4})

    assert order == ["high"] * 12 + ["low"] * 4


async def test_weights_interleave_lanes(redis, make_app):
    app = make_app(
        "fair-mixed",
        priorities=["low", "high"],
        weights={"low": 1, "high": 3},
        concurrency=1,
        prefetch=1,
        poll_block_ms=50,
        burst=True,
    )

    order = await _drain(app, {"high": 12, "low": 4})

    assert order == ["high", "high", "low", "high"] * 4


async def test_an_empty_lane_gives_its_share_away(redis, make_app):
    app = make_app(
        "fair-spare",
        priorities=["low", "high"],
        weights={"low": 1, "high": 1},
        concurrency=1,
        prefetch=1,
        poll_block_ms=50,
        burst=True,
    )

    order = await _drain(app, {"high": 3, "low": 6})

    assert order == ["high", "low"] * 3 + ["low"] * 3


def test_weights_are_read_back_lowest_first(make_app):
    app = make_app(
        "fair-read",
        priorities=["low", "default", "high"],
        weights={"high": 8, "low": 1, "default": 3},
    )

    assert list(app.weights.items()) == [("low", 1), ("default", 3), ("high", 8)]
    assert make_app("fair-none").weights is None


@pytest.mark.parametrize(
    ("weights", "message"),
    [
        ({"low": 1}, 'no entry for lane "high"'),
        ({"low": 1, "high": 2, "urgent": 3}, '"urgent"'),
        ({"low": 0, "high": 2}, "must be positive"),
        ({"low": -1, "high": 2}, "must be positive"),
    ],
)
def test_weights_must_cover_every_lane_with_a_positive_number(weights, message):
    with pytest.raises(ValueError, match=message):
        Ardiq(queue_name="fair-bad", priorities=["low", "high"], weights=weights)
