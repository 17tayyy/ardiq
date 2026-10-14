"""`ardiq status`: what a queue holds, what runs now, and when each cron fires."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from ardiq.app import _now_ms

if TYPE_CHECKING:
    from ardiq import Ardiq

RUNNING_SHOWN = 20  # the text view; --json lists them all


def parse_heartbeat(raw: str) -> dict[str, Any]:
    """A worker's heartbeat: JSON since 1.2, `"<id> running=N"` before."""
    if raw.startswith("{"):
        try:
            return json.loads(raw)
        except ValueError:
            pass
    return {"id": raw.split(" ", 1)[0]}


async def gather(app: Ardiq) -> dict[str, Any]:
    facts = await app._core.overview()
    now = _now_ms()

    by_name = {lane[0]: lane for lane in facts["lanes"]}
    lanes = []
    for name in reversed(app.priorities):
        _, length, pending, delayed, next_ms = by_name[name]
        lanes.append(
            {
                "name": name,
                "waiting": max(length - pending, 0),
                "scheduled": delayed,
                "held": pending,
                "next_scheduled": next_ms,
            }
        )

    running = [
        {"id": task_id, "name": _fn_name(app, payload), "try": tries}
        for task_id, payload, tries in sorted(facts["running"])
    ]

    crons = []
    for name, (schedule, _) in sorted(app._crons.items()):
        last = facts["crons"].get(name)
        crons.append(
            {
                "name": name,
                "next": schedule.next_after(now),
                "last": json.loads(last) if last else None,
            }
        )

    held: dict[str, int] = dict(facts["held"])
    workers = []
    for raw in facts["workers"]:
        worker = parse_heartbeat(raw)
        worker["held"] = held.pop(worker.get("id", ""), 0)
        workers.append(worker)
    workers.sort(key=lambda w: w.get("id", ""))
    # Work still held by a worker with no heartbeat waits for idle_timeout_ms.
    gone = [{"id": wid, "held": n} for wid, n in sorted(held.items()) if n]

    return {
        "queue": app.queue_name,
        "lanes": lanes,
        "running": running,
        "crons": crons,
        "workers": workers,
        "gone": gone,
        "dead": facts["dead"],
        "now": now,
    }


def render(status: dict[str, Any]) -> str:
    now = status["now"]
    lines = [f"queue  {status['queue']}", ""]

    width = max(4, *(len(lane["name"]) for lane in status["lanes"]))
    lines.append(f"{'lane':<{width}}  waiting  scheduled  held")
    for lane in status["lanes"]:
        row = (
            f"{lane['name']:<{width}}  {lane['waiting']:>7}  "
            f"{lane['scheduled']:>9}  {lane['held']:>4}"
        )
        if lane["next_scheduled"] is not None:
            row += f"  next {_when(lane['next_scheduled'])}"
        lines.append(row)

    running = status["running"]
    lines += ["", f"running  {len(running)}"]
    for task in running[:RUNNING_SHOWN]:
        lines.append(f"  {task['id']}  {task['name'] or '?'}  try {task['try']}")
    if len(running) > RUNNING_SHOWN:
        lines.append(f"  and {len(running) - RUNNING_SHOWN} more")

    if status["crons"]:
        lines += ["", "crons"]
        width = max(len(cron["name"]) for cron in status["crons"])
        for cron in status["crons"]:
            row = f"  {cron['name']:<{width}}  next {_when(cron['next'])}"
            last = cron["last"]
            if last is None:
                row += "  never ran"
            else:
                row += f"  last {last['outcome']} {_when(last['fire'])}"
                if last["tries"] > 1:
                    row += f" (try {last['tries']})"
            lines.append(row)

    lines += ["", f"workers  {len(status['workers'])}"]
    for worker in status["workers"]:
        row = f"  {worker.get('id', '?')}"
        if "host" in worker:
            row += (
                f"  {worker['host']} pid {worker['pid']}  {worker['version']}"
                f"  up {_span(now - worker['started'])}"
                f"  concurrency {worker['concurrency']}"
            )
        lines.append(row + f"  holding {worker['held']}")
    for worker in status["gone"]:
        lines.append(
            f"  {worker['id']}  gone, holding {worker['held']} until reclaimed"
        )

    lines += ["", f"dead letters  {status['dead']}"]
    return "\n".join(lines)


def _fn_name(app: Ardiq, payload: bytes | None) -> str | None:
    if payload is None:
        return None
    try:
        return app._loads(payload)["f"]
    except Exception:
        return None


def _when(ms: int) -> str:
    return f"{datetime.fromtimestamp(ms / 1000, UTC):%Y-%m-%d %H:%M:%S}Z"


def _span(ms: int) -> str:
    seconds = max(ms, 0) // 1000
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if days:
        return f"{days}d{hours}h"
    if hours:
        return f"{hours}h{minutes}m"
    if minutes:
        return f"{minutes}m{seconds}s"
    return f"{seconds}s"
