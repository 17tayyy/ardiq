"""Cron parsing, interval scheduling, and end-to-end recurring delivery."""

import asyncio
import time
from datetime import UTC, datetime, timedelta

import pytest

from ardiq.cron import _cron_next, _parse_cron, _Schedule


def _ms(y: int, mo: int, d: int, h: int, mi: int) -> int:
    return int(datetime(y, mo, d, h, mi, tzinfo=UTC).timestamp() * 1000)


# --- Cron parsing -----------------------------------------------------------


def test_cron_every_minute():
    spec = _parse_cron("* * * * *")
    # 10:00:05 → next whole minute is 10:01.
    assert _cron_next(spec, _ms(2026, 6, 13, 10, 0) + 5000) == _ms(2026, 6, 13, 10, 1)


def test_cron_step():
    spec = _parse_cron("*/5 * * * *")
    assert _cron_next(spec, _ms(2026, 6, 13, 10, 2)) == _ms(2026, 6, 13, 10, 5)
    assert _cron_next(spec, _ms(2026, 6, 13, 10, 5)) == _ms(2026, 6, 13, 10, 10)


def test_cron_daily_rolls_to_next_day():
    spec = _parse_cron("30 9 * * *")
    # past 09:30 today → tomorrow 09:30.
    assert _cron_next(spec, _ms(2026, 6, 13, 10, 0)) == _ms(2026, 6, 14, 9, 30)
    # before 09:30 today → today 09:30.
    assert _cron_next(spec, _ms(2026, 6, 13, 8, 0)) == _ms(2026, 6, 13, 9, 30)


def test_cron_lists_and_ranges():
    spec = _parse_cron("0,30 9-17 * * *")
    assert _cron_next(spec, _ms(2026, 6, 13, 9, 5)) == _ms(2026, 6, 13, 9, 30)
    assert _cron_next(spec, _ms(2026, 6, 13, 17, 31)) == _ms(2026, 6, 14, 9, 0)


def test_cron_dom_or_dow():
    # "1st of the month OR any Monday" — classic cron OR when both are restricted.
    spec = _parse_cron("0 0 1 * 1")
    # 2026-06-13 is a Saturday; the next Monday (15th) comes before the next 1st.
    assert _cron_next(spec, _ms(2026, 6, 13, 0, 0)) == _ms(2026, 6, 15, 0, 0)
    # From mid-Monday the 15th, the next hit is the following Monday, the 22nd.
    assert _cron_next(spec, _ms(2026, 6, 15, 1, 0)) == _ms(2026, 6, 22, 0, 0)


def test_cron_dow_sunday_aliases():
    sun0 = _parse_cron("0 0 * * 0")
    sun7 = _parse_cron("0 0 * * 7")
    # 2026-06-13 Sat → next Sunday is the 14th for both spellings.
    assert _cron_next(sun0, _ms(2026, 6, 13, 0, 0)) == _ms(2026, 6, 14, 0, 0)
    assert _cron_next(sun7, _ms(2026, 6, 13, 0, 0)) == _ms(2026, 6, 14, 0, 0)


@pytest.mark.parametrize(
    "expr",
    ["* * * *", "60 * * * *", "* 24 * * *", "* * 0 * *", "*/0 * * * *", "5-2 * * * *"],
)
def test_cron_rejects_invalid(expr):
    with pytest.raises(ValueError):
        _parse_cron(expr)


# --- Interval scheduling ----------------------------------------------------


def test_interval_alignment():
    s = _Schedule(every=0.3)  # 300 ms, aligned to the epoch
    assert s.next_after(0) == 300
    assert s.next_after(899) == 900
    assert s.next_after(900) == 1200  # strictly after a boundary
    assert s.next_after(1000) == 1200
    assert _Schedule(every=timedelta(seconds=2)).next_after(1000) == 2000


def test_schedule_requires_exactly_one():
    with pytest.raises(TypeError):
        _Schedule()
    with pytest.raises(TypeError):
        _Schedule(every=1, cron="* * * * *")


# --- Registration -----------------------------------------------------------


def test_cron_registers_callable_task(make_app):
    app = make_app("cronreg")

    @app.cron("*/5 * * * *")
    def report():
        return 42

    assert "report" in app.tasks
    assert report() == 42  # still a normal Task, callable inline


# --- End-to-end -------------------------------------------------------------


async def test_cron_fires_repeatedly(redis, make_app, poll):
    app = make_app("cronrep", concurrency=2, poll_block_ms=50, cron_poll_s=0.05)
    fires: list[float] = []

    @app.cron(every=0.3)
    async def tick():
        fires.append(time.monotonic())

    run = asyncio.ensure_future(app.run())
    try:
        await poll(lambda: _count_at_least(fires, 3), timeout=8)
    finally:
        app.stop()
        await asyncio.wait_for(run, timeout=5)

    assert len(fires) >= 3
    # A 300 ms cadence can't have produced an absurd number over the window.
    assert len(fires) <= 20


async def test_cron_occurrence_dedup(redis, make_app):
    # The same occurrence id staged twice must run exactly once (SET NX dedup).
    app = make_app("crondedup", concurrency=2, poll_block_ms=50, burst=True)
    runs: list[int] = []

    @app.cron(every=10)
    async def job():
        runs.append(1)

    fire = int(time.time() * 1000) - 1  # already due → drained in burst
    await app._enqueue_cron("job", fire, None)
    await app._enqueue_cron("job", fire, None)
    await asyncio.wait_for(app.run(), timeout=15)

    assert runs == [1]


async def _count_at_least(seq, n: int) -> bool:
    return len(seq) >= n
