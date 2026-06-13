"""Recurring schedules: a lean 5-field cron parser and fixed intervals.

Both forms collapse to ``next_after(now_ms) -> ms`` — the next fire strictly
after a given moment. Cron expressions are evaluated in UTC.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


def _interval_ms(every: timedelta | float) -> int:
    """Interval in ms from a timedelta or a number of seconds."""
    secs = every.total_seconds() if isinstance(every, timedelta) else float(every)
    ms = round(secs * 1000)
    if ms <= 0:
        raise ValueError("cron interval must be positive")
    return ms


def _parse_field(spec: str, lo: int, hi: int) -> frozenset[int]:
    """One cron field → the set of values it matches, within range [lo, hi].

    Handles `*`, single values, ranges `a-b`, and steps `*/n` / `a-b/n`.
    """
    values: set[int] = set()
    for part in spec.split(","):
        step = 1
        if "/" in part:
            part, _, step_s = part.partition("/")
            step = int(step_s)
            if step <= 0:
                raise ValueError(f"invalid cron step in {spec!r}")
        if part == "*":
            start, end = lo, hi
        elif "-" in part:
            start_s, _, end_s = part.partition("-")
            start, end = int(start_s), int(end_s)
        else:
            start = end = int(part)
        if not (lo <= start <= hi and lo <= end <= hi and start <= end):
            raise ValueError(f"cron field {spec!r} out of range [{lo}, {hi}]")
        values.update(range(start, end + 1, step))
    if not values:
        raise ValueError(f"empty cron field {spec!r}")
    return frozenset(values)


@dataclass(frozen=True, slots=True)
class _CronSpec:
    minutes: frozenset[int]
    hours: frozenset[int]
    doms: frozenset[int]
    months: frozenset[int]
    dows: frozenset[int]
    dom_restricted: bool
    dow_restricted: bool


def _parse_cron(expr: str) -> _CronSpec:
    """Parse a standard 5-field cron expression (minute hour dom month dow), UTC.

    Day-of-week is 0-6 with Sunday=0; 7 is also accepted as Sunday.
    """
    fields = expr.split()
    if len(fields) != 5:
        raise ValueError(f"cron needs 5 fields, got {len(fields)}: {expr!r}")
    minute, hour, dom, month, dow = fields
    return _CronSpec(
        minutes=_parse_field(minute, 0, 59),
        hours=_parse_field(hour, 0, 23),
        doms=_parse_field(dom, 1, 31),
        months=_parse_field(month, 1, 12),
        dows=frozenset(d % 7 for d in _parse_field(dow, 0, 7)),  # 7 → Sunday
        dom_restricted=dom != "*",
        dow_restricted=dow != "*",
    )


def _cron_matches(spec: _CronSpec, dt: datetime) -> bool:
    if dt.minute not in spec.minutes or dt.hour not in spec.hours:
        return False
    if dt.month not in spec.months:
        return False
    dom_ok = dt.day in spec.doms
    dow_ok = (dt.weekday() + 1) % 7 in spec.dows  # Mon=0..Sun=6 → Sun=0..Sat=6
    if spec.dom_restricted and spec.dow_restricted:
        return dom_ok or dow_ok
    return dom_ok and dow_ok


def _cron_next(spec: _CronSpec, after_ms: int) -> int:
    """First fire time (epoch ms, UTC) strictly after `after_ms`."""
    start = datetime.fromtimestamp(after_ms / 1000, UTC)
    dt = start.replace(second=0, microsecond=0) + timedelta(minutes=1)
    limit = dt + timedelta(days=366 * 5)
    while dt <= limit:
        if _cron_matches(spec, dt):
            return round(dt.timestamp() * 1000)
        dt += timedelta(minutes=1)
    raise ValueError("cron expression never matches within 5 years")


class _Schedule:
    """A recurring schedule: the next fire time after a given moment (epoch ms)."""

    __slots__ = ("_cron", "_interval_ms")

    def __init__(
        self, *, every: timedelta | float | None = None, cron: str | None = None
    ):
        if (every is None) == (cron is None):
            raise TypeError("@cron needs exactly one of `spec` or `every`")
        self._interval_ms: int | None = (
            _interval_ms(every) if every is not None else None
        )
        self._cron: _CronSpec | None = _parse_cron(cron) if cron is not None else None

    def next_after(self, now_ms: int) -> int:
        if self._cron is not None:
            return _cron_next(self._cron, now_ms)
        assert self._interval_ms is not None
        ms = self._interval_ms
        return ((now_ms // ms) + 1) * ms
