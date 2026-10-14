"""`ardiq status` and `ardiq purge`: what a queue holds, and dropping what waits."""

import asyncio
import io
import json
import os

import pytest

from ardiq import cli
from ardiq.status import gather, parse_heartbeat, render


def test_parse_heartbeat_reads_json_and_the_old_format():
    assert parse_heartbeat('{"id": "w1", "pid": 7}') == {"id": "w1", "pid": 7}
    assert parse_heartbeat("w1 running=2") == {"id": "w1"}
    assert parse_heartbeat("{broken") == {"id": "{broken"}


def test_render_shows_each_section():
    status = {
        "queue": "q",
        "now": 7_200_000,
        "lanes": [
            {
                "name": "high",
                "waiting": 2,
                "scheduled": 1,
                "held": 1,
                "next_scheduled": 0,
            },
            {
                "name": "low",
                "waiting": 0,
                "scheduled": 0,
                "held": 0,
                "next_scheduled": None,
            },
        ],
        "running": [{"id": f"t{i}", "name": "work", "try": 1} for i in range(22)],
        "crons": [
            {"name": "nightly", "next": 3_600_000, "last": None},
            {
                "name": "poll",
                "next": 3_600_000,
                "last": {"fire": 0, "outcome": "failed", "tries": 3, "finished": 1},
            },
        ],
        "workers": [
            {
                "id": "w1",
                "host": "box",
                "pid": 7,
                "version": "1.2.0",
                "started": 0,
                "concurrency": 4,
                "held": 2,
            },
            {"id": "w0", "held": 0},  # an older worker: id only
        ],
        "gone": [{"id": "w9", "held": 3}],
        "dead": 5,
    }

    out = render(status)

    assert "high        2          1     1  next 1970-01-01 00:00:00Z" in out
    assert "running  22" in out and "t0  work  try 1" in out and "and 2 more" in out
    assert "nightly  next 1970-01-01 01:00:00Z  never ran" in out
    assert "last failed 1970-01-01 00:00:00Z (try 3)" in out
    assert "w1  box pid 7  1.2.0  up 2h0m  concurrency 4  holding 2" in out
    assert "  w0  holding 0" in out
    assert "w9  gone, holding 3 until reclaimed" in out
    assert "dead letters  5" in out


async def test_status_reads_lanes_running_tasks_and_workers(redis, make_app, poll):
    app = make_app("status", priorities=["low", "high"], concurrency=1)
    started = asyncio.Event()
    finish = asyncio.Event()

    @app.task()
    async def hold():
        started.set()
        await finish.wait()

    @app.task()
    async def later():
        pass

    @app.cron(every=3600, priority="high")
    async def hourly():
        pass

    held = await hold.options(priority="high").enqueue()
    await later.options(priority="low").enqueue()
    await later.options(priority="low", delay_ms=60_000).enqueue()
    run = asyncio.ensure_future(app.run())
    try:
        await asyncio.wait_for(started.wait(), timeout=5)

        async def worker_seen():
            return bool((await gather(app))["workers"])

        assert await poll(worker_seen)
        status = await gather(app)
    finally:
        finish.set()
        app.stop()
        await asyncio.wait_for(run, timeout=15)

    assert [lane["name"] for lane in status["lanes"]] == ["high", "low"]
    low = status["lanes"][1]
    assert (low["waiting"], low["scheduled"], low["held"]) == (1, 1, 0)
    assert low["next_scheduled"] is not None
    assert status["running"] == [{"id": held.id, "name": "hold", "try": 1}]
    assert status["crons"][0]["name"] == "hourly"
    assert status["crons"][0]["last"] is None
    assert status["crons"][0]["next"] % 3_600_000 == 0

    (worker,) = status["workers"]
    assert worker["id"] == app.worker_id and worker["pid"] == os.getpid()
    assert worker["lanes"] == ["low", "high"] and worker["concurrency"] == 1
    assert worker["held"] == 1
    assert status["gone"] == [] and status["dead"] == 0


async def test_status_lists_work_held_by_a_worker_without_a_heartbeat(redis, make_app):
    app = make_app("status_gone")

    @app.task()
    async def noop():
        pass

    await noop.enqueue()
    stream = "ardiq:status_gone:queues:default"
    await redis.xgroup_create(stream, "workers", id="0", mkstream=True)
    await redis.xreadgroup("workers", "ghost", {stream: ">"}, count=1)

    status = await gather(app)

    assert status["gone"] == [{"id": "ghost", "held": 1}]
    assert status["lanes"][0]["held"] == 1 and status["lanes"][0]["waiting"] == 0


async def test_a_cron_run_is_recorded_once_it_settles(redis, make_app):
    app = make_app("status_cron")
    calls = []

    @app.cron(every=60, max_retries=1)
    async def flaky():
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("first try fails")

    payload = app._pack("flaky", (), {})
    await app._execute("cron:flaky:120000", payload, 1)  # retried: not recorded
    assert (await gather(app))["crons"][0]["last"] is None

    await app._execute("cron:flaky:120000", payload, 2)
    await app._execute("manual-run", payload, 1)  # not an occurrence

    last = (await gather(app))["crons"][0]["last"]
    assert (last["fire"], last["outcome"], last["tries"]) == (120000, "succeeded", 2)


async def _held_and_waiting(app):
    started = asyncio.Event()
    finish = asyncio.Event()

    @app.task()
    async def hold():
        started.set()
        await finish.wait()

    @app.task(unique=True)
    async def once(n):
        pass

    held = await hold.options(priority="high").enqueue()
    dropped = [await once.options(priority="low").enqueue(n) for n in range(3)]
    dropped += [
        await once.options(priority="low", delay_ms=60_000).enqueue(n) for n in (3, 4)
    ]
    dropped.append(await once.options(priority="high").enqueue(5))
    run = asyncio.ensure_future(app.run())
    await asyncio.wait_for(started.wait(), timeout=5)
    return held, dropped, once, finish, run


async def test_purge_drops_waiting_and_scheduled_work_but_not_running(redis, make_app):
    app = make_app("purge", priorities=["low", "high"], concurrency=1)
    held, dropped, once, finish, run = await _held_and_waiting(app)
    try:
        assert await app._core.purge() == [("low", 3, 2), ("high", 1, 0)]
        assert {await app.status(job.id) for job in dropped} == {"not_found"}
        assert await app.status(held.id) == "running"
        # The unique lock went with the task, so the same call enqueues again.
        again = await once.options(priority="low").enqueue(0)
        assert await app.status(again.id) == "queued"
    finally:
        finish.set()
        app.stop()
        await asyncio.wait_for(run, timeout=15)
    assert (await held.result()).success


async def test_purge_one_lane_leaves_the_others(redis, make_app):
    app = make_app("purge_lane", priorities=["low", "high"], concurrency=1)
    _, dropped, _, finish, run = await _held_and_waiting(app)
    try:
        assert await app._core.purge("low") == [("low", 3, 2)]
        assert await app.status(dropped[-1].id) == "queued"
        with pytest.raises(ValueError, match='no lane "mid"'):
            await app._core.purge("mid")
    finally:
        finish.set()
        app.stop()
        await asyncio.wait_for(run, timeout=15)


class _Terminal(io.StringIO):
    def isatty(self):
        return True


@pytest.fixture
def cli_app(make_app, monkeypatch):
    app = make_app("purge_cli", priorities=["low", "high"])
    monkeypatch.setattr(cli, "import_string", lambda path: app)

    @app.task()
    async def noop():
        pass

    return app, noop


async def _enqueue_two(noop):
    await noop.options(priority="low").enqueue()
    await noop.options(priority="high", delay_ms=60_000).enqueue()


def test_purge_cli_refuses_without_a_terminal_or_yes(redis, cli_app, capsys):
    app, noop = cli_app
    asyncio.run(_enqueue_two(noop))

    with pytest.raises(SystemExit) as exc:
        cli.main(["purge", "x:app"])

    assert exc.value.code == 1
    err = capsys.readouterr().err
    assert "about to drop 1 waiting and 1 scheduled tasks from queue 'purge_cli'" in err
    assert "pass --yes" in err
    assert asyncio.run(app.queue_size()) == 2


@pytest.mark.parametrize(("answer", "purged"), [("y\n", True), ("\n", False)])
def test_purge_cli_asks_on_a_terminal(
    redis, cli_app, capsys, monkeypatch, answer, purged
):
    app, noop = cli_app
    asyncio.run(_enqueue_two(noop))
    monkeypatch.setattr("sys.stdin", _Terminal(answer))

    if purged:
        cli.main(["purge", "x:app"])
    else:
        with pytest.raises(SystemExit):
            cli.main(["purge", "x:app"])

    err = capsys.readouterr().err
    assert ("high: dropped 0 waiting, 1 scheduled" in err) is purged
    assert asyncio.run(app.queue_size()) == (0 if purged else 2)


def test_purge_cli_with_yes_and_a_lane(redis, cli_app, capsys):
    app, noop = cli_app
    asyncio.run(_enqueue_two(noop))

    cli.main(["purge", "x:app", "--lane", "low", "--yes"])

    assert capsys.readouterr().err == "low: dropped 1 waiting, 0 scheduled\n"
    assert asyncio.run(app.queue_size()) == 1
    with pytest.raises(SystemExit, match="no lane 'mid'"):
        cli.main(["purge", "x:app", "--lane", "mid"])


def test_purge_cli_with_nothing_waiting_does_not_ask(redis, cli_app, capsys):
    cli.main(["purge", "x:app"])

    assert capsys.readouterr().err == "nothing to purge\n"


def test_status_cli_prints_text_or_json(redis, cli_app, capsys):
    _, noop = cli_app
    asyncio.run(_enqueue_two(noop))

    cli.main(["status", "x:app"])
    assert "queue  purge_cli" in capsys.readouterr().out

    cli.main(["status", "x:app", "--json"])
    status = json.loads(capsys.readouterr().out)
    assert [lane["waiting"] for lane in status["lanes"]] == [0, 1]
