"""Multiprocess workers: `ardiq run --workers N` and its supervisor."""

import os
import subprocess
import sys

import pytest

from ardiq import cli
from ardiq.cli import build_parser

WORKER_MODULE = """
import os

from ardiq import Ardiq

app = Ardiq(
    redis_url=os.environ["ARDIQ_TEST_REDIS"],
    queue_name="workers_e2e",
    poll_block_ms=100,
)


@app.task()
async def note(n: int):
    return n
"""


class _FakeChild:
    """A child process that reports `code`, or runs until terminated."""

    def __init__(self, pid: int, code: int | None):
        self.pid = pid
        self.code = code
        self.terminated = False

    def poll(self) -> int | None:
        return -15 if self.terminated else self.code

    def terminate(self) -> None:
        self.terminated = True


@pytest.fixture
def spawned(monkeypatch):
    """Collect the argv of every child the supervisor would start."""
    monkeypatch.setattr(cli, "POLL_CHILDREN_S", 0.001)
    calls: list[list[str]] = []
    children: list[_FakeChild] = []

    def fake_popen(argv):
        calls.append(argv)
        child = children[len(calls) - 1] if len(calls) <= len(children) else None
        if child is None:
            child = _FakeChild(1000 + len(calls), 0)
            children.append(child)
        return child

    monkeypatch.setattr(cli.subprocess, "Popen", fake_popen)
    return calls, children


def _args(*extra: str):
    return build_parser().parse_args(["run", "myapp:app", "-q", *extra])


def test_workers_defaults_to_one():
    assert _args().workers == 1


def test_workers_must_be_positive():
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(["run", "myapp:app", "--workers", "0"])
    assert exc.value.code != 0


def test_one_worker_stays_in_this_process(monkeypatch):
    supervised = []
    monkeypatch.setattr(cli, "_supervise", lambda args: supervised.append(args))
    monkeypatch.setattr(cli, "import_string", lambda path: object())
    monkeypatch.setattr(cli.asyncio, "run", lambda coro: coro.close())

    cli._run(_args())

    assert supervised == []


def test_the_supervisor_starts_one_child_per_worker(spawned):
    calls, _ = spawned

    cli._supervise(_args("--workers", "3", "--burst"))

    assert len(calls) == 3
    assert calls[0][:2] == [sys.executable, "-m"]
    assert calls[0][2:] == ["ardiq", "run", "myapp:app", "--quiet", "--burst"]


def test_a_child_never_inherits_workers(spawned):
    # It would spawn its own children, and theirs, and so on.
    calls, _ = spawned

    cli._supervise(_args("--workers", "2"))

    assert all("--workers" not in argv and "-w" not in argv for argv in calls)


def test_verbose_reaches_the_children(spawned):
    calls, _ = spawned

    cli._supervise(_args("--workers", "2", "--verbose"))

    assert all("--verbose" in argv for argv in calls)


def test_a_dead_worker_takes_the_others_down(spawned):
    _, children = spawned
    children.extend([_FakeChild(1, 1), _FakeChild(2, None), _FakeChild(3, None)])

    with pytest.raises(SystemExit) as exc:
        cli._supervise(_args("--workers", "3"))

    assert exc.value.code == 1
    assert children[1].terminated and children[2].terminated


def test_a_clean_drain_exits_zero(spawned):
    _, children = spawned
    children.extend([_FakeChild(1, 0), _FakeChild(2, 0)])

    cli._supervise(_args("--workers", "2", "--burst"))  # no SystemExit

    assert not any(child.terminated for child in children)


async def test_two_worker_processes_drain_the_queue(redis, make_app, tmp_path):
    (tmp_path / "workers_app.py").write_text(WORKER_MODULE)
    app = make_app("workers_e2e", poll_block_ms=100)
    note = app.ref("note")
    jobs = [await note.enqueue(i) for i in range(20)]

    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "ardiq",
            "run",
            "workers_app:app",
            "--burst",
            "--workers",
            "2",
            "-q",
        ],
        env={
            **os.environ,
            "PYTHONPATH": str(tmp_path),
            "ARDIQ_TEST_REDIS": app.redis_url,
        },
        capture_output=True,
        timeout=120,
    )

    assert proc.returncode == 0, proc.stderr.decode()
    results = [await job.result() for job in jobs]
    assert [r.value for r in results] == list(range(20))


def test_more_than_one_worker_goes_to_the_supervisor(monkeypatch):
    seen = []
    monkeypatch.setattr(cli, "_supervise", lambda args: seen.append(args.workers))

    cli._run(_args("--workers", "2"))

    assert seen == [2]
