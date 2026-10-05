"""`ardiq doctor`: verdicts from the core's facts, and the CLI against Redis."""

import asyncio
import json

import pytest

from ardiq import Ardiq
from ardiq.cli import main
from ardiq.doctor import _redact, diagnose, judge

URL = "redis://localhost:6379"
LANES = ["low", "high"]


def facts(**over):
    base = {
        "latency_ms": 0.4,
        "version": "7.4.1",
        "lanes": [("high", 0, 0, (1, 0)), ("low", 3, 1, (1, 2))],
        "workers": ["w1 running=2"],
        "write": None,
    }
    return {**base, **over}


def by_name(checks):
    return {c.name: c for c in checks}


def check(name, **over):
    return by_name(judge(facts(**over), LANES, URL))[name]


def test_healthy_app_is_all_ok():
    checks = judge(facts(), LANES, URL)
    assert [c.name for c in checks] == [
        "redis",
        "version",
        "write",
        "lane high",
        "lane low",
        "workers",
    ]
    assert {c.status for c in checks} == {"ok"}
    assert by_name(checks)["lane low"].detail == "3 queued, 1 delayed, 2 in flight"


@pytest.mark.parametrize(
    ("version", "status"),
    [("6.2.0", "ok"), ("8.0.2", "ok"), ("6.0.16", "fail"), ("5.0.7", "fail")],
)
def test_version_floor(version, status):
    assert check("version", version=version).status == status


def test_unreadable_version_warns():
    f = facts()
    del f["version"]
    f["version_error"] = "NOPERM"
    assert by_name(judge(f, LANES, URL))["version"].status == "warn"
    assert check("version", version="unstable").status == "warn"


def test_slow_redis_warns():
    assert check("redis", latency_ms=250.0).status == "warn"


def test_refused_write_fails():
    f = facts()
    del f["write"]
    f["write_error"] = "NOPERM this user has no permissions to run the 'eval' command"
    write = by_name(judge(f, LANES, URL))["write"]
    assert write.status == "fail"
    assert "eval" in write.detail


def test_lane_without_group():
    lanes = [("high", 0, 0, (1, 0)), ("low", 5, 0, None)]
    # Live workers made their groups on start, so they read other lanes.
    assert check("lane low", lanes=lanes).status == "warn"
    # Nothing has started yet: that is a fresh deploy, not a fault.
    assert check("lane low", lanes=lanes, workers=[]).status == "ok"


def test_no_workers_warns():
    assert check("workers", workers=[]).status == "warn"


def test_redact_hides_the_password():
    assert _redact("redis://:pw@host:6379/1") == "redis://:***@host:6379/1"
    assert _redact("rediss://me:pw@host") == "rediss://me:***@host"
    assert _redact("redis://host:6379") == "redis://host:6379"


async def test_unreachable_redis_fails_fast():
    app = Ardiq(redis_url="redis://localhost:1")
    checks = await asyncio.wait_for(diagnose(app), 10)
    assert [(c.name, c.status) for c in checks] == [("redis", "fail")]


async def test_doctor_against_redis(redis, make_app):
    app = make_app("doc", priorities=LANES)

    checks = by_name(await diagnose(app))
    assert checks["redis"].status == "ok"
    assert checks["version"].status == "ok"
    assert checks["write"].status == "ok"
    assert checks["workers"].status == "warn"
    assert await redis.keys("ardiq:doc:doctor:*") == []  # the probe cleans up

    worker = make_app("doc", priorities=LANES)
    run = asyncio.create_task(worker.run())
    try:
        for _ in range(100):
            checks = by_name(await diagnose(app))
            if checks["workers"].status == "ok":
                break
            await asyncio.sleep(0.05)
        assert worker.worker_id in checks["workers"].detail
        assert checks["lane low"].status == "ok"
    finally:
        worker.stop()
        await run
    # A clean stop takes the worker off the list at once.
    assert by_name(await diagnose(app))["workers"].status == "warn"


async def test_lane_no_live_worker_reads(redis, make_app):
    producer = make_app("doc", priorities=["low", "high", "extra"])
    await producer._core.enqueue("t1", b"x", "extra")
    worker = make_app("doc", priorities=LANES)
    run = asyncio.create_task(worker.run())
    try:
        for _ in range(100):
            checks = by_name(await diagnose(producer))
            if checks["workers"].status == "ok":
                break
            await asyncio.sleep(0.05)
        extra = checks["lane extra"]
        assert extra.status == "warn"
        assert extra.detail.startswith("1 queued")
    finally:
        worker.stop()
        await run


def test_cli_json_and_exit_code(redis, make_app, capsys, monkeypatch):
    monkeypatch.setattr("ardiq.cli.import_string", lambda _: make_app("doc"))
    main(["doctor", "x:app", "--json"])
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] is True
    assert {c["name"] for c in out["checks"]} >= {"redis", "version", "write"}

    monkeypatch.setattr(
        "ardiq.cli.import_string", lambda _: Ardiq(redis_url="redis://localhost:1")
    )
    with pytest.raises(SystemExit) as exc:
        main(["doctor", "x:app"])
    assert exc.value.code == 1
    assert capsys.readouterr().out.startswith("fail  redis")
