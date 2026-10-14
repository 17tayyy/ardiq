"""Cron parsing, interval scheduling, and end-to-end recurring delivery."""

import asyncio
import time
from datetime import UTC, datetime, timedelta, timezone

import pytest

from ardiq.cron import _cron_next, _parse_cron, _Schedule


def _ms(y: int, mo: int, d: int, h: int, mi: int) -> int:
    return int(datetime(y, mo, d, h, mi, tzinfo=UTC).timestamp() * 1000)


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


def test_cron_registers_callable_task(make_app):
    app = make_app("cronreg")

    @app.cron("*/5 * * * *")
    def report():
        return 42

    assert "report" in app.tasks
    assert report() == 42  # still a normal Task, callable inline


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


def test_a_cron_schedule_fires_on_its_expression():
    schedule = _Schedule(cron="*/5 * * * *")

    # 1970-01-01T00:02Z -> the next multiple of five minutes
    assert schedule.next_after(120_000) == 300_000


@pytest.mark.parametrize("every", [0, -1, timedelta(0)])
def test_an_interval_must_be_positive(every):
    with pytest.raises(ValueError, match="positive"):
        _Schedule(every=every)


def test_a_month_that_never_comes_is_refused():
    # 30 February: valid as a field, unreachable as a date.
    with pytest.raises(ValueError, match="never matches"):
        _cron_next(_parse_cron("0 0 30 2 *"), 0)


def test_the_month_field_excludes_other_months():
    spec = _parse_cron("0 0 1 3 *")  # midnight, 1 March

    assert _cron_next(spec, 0) == round(
        datetime(1970, 3, 1, tzinfo=UTC).timestamp() * 1000
    )


def test_previous_before_walks_back_one_occurrence():
    s = _Schedule(cron="*/10 * * * *")
    assert s.previous_before(_ms(2026, 6, 13, 12, 0)) == _ms(2026, 6, 13, 11, 50)
    assert s.previous_before(_ms(2026, 6, 13, 12, 5) + 30_000) == _ms(
        2026, 6, 13, 12, 0
    )
    daily = _Schedule(cron="0 3 * * *")
    assert daily.previous_before(_ms(2026, 6, 13, 3, 0)) == _ms(2026, 6, 12, 3, 0)
    assert _Schedule(every=0.3).previous_before(1200) == 900


async def _waiting_occurrences(redis, queue: str) -> int:
    return len([k async for k in redis.scan_iter(f"ardiq:{queue}:task:data:cron:*")])


@pytest.mark.parametrize("coalesce", [True, False])
async def test_a_busy_worker_stacks_occurrences_unless_coalesced(
    redis, make_app, poll, coalesce
):
    # Issue #3: while the only slot is busy, every occurrence used to queue up
    # behind it and run back to back once it freed.
    queue = f"cron_coalesce_{coalesce}"
    app = make_app(queue, concurrency=1, poll_block_ms=50, cron_poll_s=0.05)
    started = asyncio.Event()
    finish = asyncio.Event()
    ticks: list[float] = []

    @app.task()
    async def hold():
        started.set()
        await finish.wait()

    @app.cron(every=0.2, coalesce=coalesce)
    async def tick():
        ticks.append(time.monotonic())

    await hold.enqueue()
    run = asyncio.ensure_future(app.run())
    try:
        await asyncio.wait_for(started.wait(), timeout=5)
        await asyncio.sleep(1.2)  # about six periods while the slot is taken
        waiting = await _waiting_occurrences(redis, queue)
        if coalesce:
            assert waiting == 1
        else:
            assert waiting >= 4
        finish.set()
        assert await poll(lambda: _count_at_least(ticks, 1))  # the waiting one runs
    finally:
        finish.set()
        app.stop()
        await asyncio.wait_for(run, timeout=5)


async def test_a_fresh_worker_does_not_stack_on_an_occurrence_left_waiting(
    redis, make_app
):
    queue = "cron_coalesce_fresh"
    app = make_app(queue, concurrency=1, poll_block_ms=50, cron_poll_s=0.05)
    started = asyncio.Event()
    finish = asyncio.Event()

    @app.task()
    async def hold():
        started.set()
        await finish.wait()

    @app.cron(every=3600, coalesce=True)
    async def hourly():
        pass

    schedule = app._crons["hourly"][0]
    last = schedule.previous_before(schedule.next_after(int(time.time() * 1000)))
    await hold.enqueue()
    await app._enqueue_cron("hourly", last, None)  # left by a worker that died
    run = asyncio.ensure_future(app.run())
    try:
        await asyncio.wait_for(started.wait(), timeout=5)
        await asyncio.sleep(0.3)
        assert await _waiting_occurrences(redis, queue) == 1
    finally:
        finish.set()
        app.stop()
        await asyncio.wait_for(run, timeout=5)


MADRID = "Europe/Madrid"  # 2026: CET to CEST on Mar 29, back on Oct 25, at 01:00 UTC


def test_a_cron_in_a_time_zone_follows_its_wall_clock():
    s = _Schedule(cron="0 10,22 * * *", tz=MADRID)
    assert s.next_after(_ms(2026, 7, 1, 0, 0)) == _ms(2026, 7, 1, 8, 0)  # UTC+2
    assert s.next_after(_ms(2026, 12, 1, 0, 0)) == _ms(2026, 12, 1, 9, 0)  # UTC+1


def test_a_time_skipped_by_spring_forward_fires_when_the_gap_ends():
    s = _Schedule(cron="30 2 * * *", tz=MADRID)
    # 02:30 does not exist on Mar 29: it runs at 03:00 local instead.
    assert s.next_after(_ms(2026, 3, 28, 12, 0)) == _ms(2026, 3, 29, 1, 0)
    assert s.next_after(_ms(2026, 3, 29, 1, 0)) == _ms(2026, 3, 30, 0, 30)
    assert s.previous_before(_ms(2026, 3, 30, 0, 30)) == _ms(2026, 3, 29, 1, 0)


def test_a_time_repeated_by_fall_back_fires_once():
    s = _Schedule(cron="30 2 * * *", tz=MADRID)
    # 02:30 happens twice on Oct 25; only the first one fires.
    assert s.next_after(_ms(2026, 10, 24, 12, 0)) == _ms(2026, 10, 25, 0, 30)
    assert s.next_after(_ms(2026, 10, 25, 0, 30)) == _ms(2026, 10, 26, 1, 30)
    assert s.previous_before(_ms(2026, 10, 26, 1, 30)) == _ms(2026, 10, 25, 0, 30)


def _fires(s: _Schedule, after: int, n: int) -> list[int]:
    fires = []
    for _ in range(n):
        after = s.next_after(after)
        fires.append(after)
    return fires


def test_a_rate_follows_real_time_across_dst():
    s = _Schedule(cron="*/30 * * * *", tz=MADRID)
    # Fall back: local 02:00 and 02:30 come twice, and so do the fires.
    assert _fires(s, _ms(2026, 10, 24, 23, 59), 4) == [
        _ms(2026, 10, 25, 0, 0),
        _ms(2026, 10, 25, 0, 30),
        _ms(2026, 10, 25, 1, 0),
        _ms(2026, 10, 25, 1, 30),
    ]
    # Spring forward: half an hour of real time after 01:30 local is 03:00.
    assert _fires(s, _ms(2026, 3, 29, 0, 15), 2) == [
        _ms(2026, 3, 29, 0, 30),
        _ms(2026, 3, 29, 1, 0),
    ]


def test_tz_takes_a_tzinfo_and_refuses_bad_input():
    india = timezone(timedelta(hours=5, minutes=30))
    s = _Schedule(cron="0 9 * * *", tz=india)
    assert s.next_after(_ms(2026, 1, 1, 0, 0)) == _ms(2026, 1, 1, 3, 30)
    with pytest.raises(TypeError, match="tz applies to a cron spec"):
        _Schedule(every=60, tz=MADRID)
    with pytest.raises(ValueError, match="pip install tzdata"):
        _Schedule(cron="0 9 * * *", tz="Europe/Atlantis")


def test_next_after_reuses_its_answer_only_before_it():
    s = _Schedule(cron="0 * * * *")
    assert s.next_after(_ms(2026, 6, 13, 10, 30)) == _ms(2026, 6, 13, 11, 0)
    assert s.next_after(_ms(2026, 6, 13, 10, 59)) == _ms(2026, 6, 13, 11, 0)
    assert s.next_after(_ms(2026, 6, 13, 9, 30)) == _ms(2026, 6, 13, 10, 0)
    assert s.next_after(_ms(2026, 6, 13, 11, 0)) == _ms(2026, 6, 13, 12, 0)


def test_app_cron_takes_a_time_zone(make_app):
    app = make_app("cron_tz")

    @app.cron("0 10 * * *", tz=MADRID)
    async def digest():
        pass

    schedule = app._crons["digest"][0]
    assert schedule.next_after(_ms(2026, 7, 1, 0, 0)) == _ms(2026, 7, 1, 8, 0)
    with pytest.raises(ValueError, match="unknown time zone"):
        app.cron("0 10 * * *", tz="Mars/Olympus")
