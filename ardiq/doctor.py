"""`ardiq doctor`: check that an app can reach and use its Redis."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, NamedTuple
from urllib.parse import urlsplit, urlunsplit

from ardiq._core import ArdiqError
from ardiq.status import parse_heartbeat

if TYPE_CHECKING:
    from ardiq import Ardiq

MIN_REDIS = (6, 2)  # XAUTOCLAIM
SLOW_PING_MS = 100.0
TIMEOUT_MS = 5000

Status = Literal["ok", "warn", "fail"]


class Check(NamedTuple):
    name: str
    status: Status
    detail: str


async def diagnose(app: Ardiq) -> list[Check]:
    target = _redact(app.redis_url)
    try:
        facts = await app._core.doctor(TIMEOUT_MS)
    except ArdiqError as exc:
        return [Check("redis", "fail", f"{target}: {exc}")]
    return judge(facts, app.priorities, target)


def judge(facts: dict[str, Any], priorities: list[str], target: str) -> list[Check]:
    checks = [_reach(facts["latency_ms"], target), _version(facts)]
    if "write_error" in facts:
        checks.append(Check("write", "fail", facts["write_error"]))
    else:
        checks.append(Check("write", "ok", "keys and Lua scripts allowed"))

    workers = facts.get("workers")
    if "lanes_error" in facts:
        checks.append(Check("lanes", "fail", facts["lanes_error"]))
    else:
        by_name = {lane[0]: lane for lane in facts["lanes"]}
        for priority in reversed(priorities):
            checks.append(_lane(by_name[priority], bool(workers)))

    if "workers_error" in facts:
        checks.append(Check("workers", "fail", facts["workers_error"]))
    elif workers:
        ids = sorted(parse_heartbeat(w).get("id", "?") for w in workers)
        checks.append(Check("workers", "ok", f"{len(workers)} live: " + ", ".join(ids)))
    else:
        checks.append(Check("workers", "warn", "none live on this queue"))
    return checks


def _reach(latency_ms: float, target: str) -> Check:
    detail = f"{target} ({latency_ms:.1f} ms)"
    if latency_ms > SLOW_PING_MS:
        return Check(
            "redis", "warn", f"{detail}, slow: every task costs a few round trips"
        )
    return Check("redis", "ok", detail)


def _version(facts: dict[str, Any]) -> Check:
    if "version_error" in facts:
        return Check("version", "warn", f"could not read it: {facts['version_error']}")
    raw = facts["version"]
    try:
        found = tuple(int(part) for part in raw.split(".")[:2])
    except ValueError:
        return Check("version", "warn", f"unrecognised version {raw!r}")
    need = ".".join(map(str, MIN_REDIS))
    if found < MIN_REDIS:
        return Check("version", "fail", f"{raw}, ArdiQ needs {need} or newer")
    return Check("version", "ok", raw)


def _lane(lane: tuple[str, int, int, tuple[int, int] | None], workers: bool) -> Check:
    priority, queued, delayed, group = lane
    name = f"lane {priority}"
    counts = f"{queued} queued, {delayed} delayed"
    if group is not None:
        return Check(name, "ok", f"{counts}, {group[1]} in flight")
    if workers:
        # Live workers create the group on start: they read other lanes.
        return Check(name, "warn", f"{counts}, live workers do not read this lane")
    return Check(name, "ok", f"{counts}, no worker has started on it yet")


def _redact(url: str) -> str:
    parts = urlsplit(url)
    if parts.password is None:
        return url
    host = parts.netloc.rpartition("@")[2]
    user = parts.username or ""
    return urlunsplit(parts._replace(netloc=f"{user}:***@{host}"))
