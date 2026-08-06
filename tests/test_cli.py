"""CLI: app loading and `ardiq run --burst`."""

import asyncio
import logging
import os

import pytest

from ardiq.cli import build_parser, import_string, main, serve


def test_cli_help_lists_run(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["--help"])
    assert exc.value.code == 0
    assert "run" in capsys.readouterr().out


def test_cli_no_command_exits_nonzero(capsys):
    with pytest.raises(SystemExit) as exc:
        main([])
    assert exc.value.code == 1
    assert "run" in capsys.readouterr().err


def test_cli_run_requires_app():
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(["run"])
    assert exc.value.code != 0


def test_cli_run_parses_flags():
    args = build_parser().parse_args(["run", "myapp:app", "--burst", "-q"])
    assert args.command == "run"
    assert args.app == "myapp:app"
    assert args.burst is True
    assert args.quiet is True
    assert args.verbose is False


def test_import_string_loads_attr():
    assert import_string("os:getcwd") is os.getcwd


def test_import_string_requires_colon():
    with pytest.raises(ValueError):
        import_string("os")


def test_import_string_missing_attr():
    with pytest.raises(ValueError):
        import_string("os:does_not_exist")


async def test_serve_burst_runs_to_completion(redis, make_app):
    app = make_app("cli", concurrency=2, poll_block_ms=50)

    @app.task()
    def add(a, b):
        return a + b

    job = await add.enqueue(2, 3)
    await asyncio.wait_for(serve(app, burst=True, quiet=True), timeout=15)

    res = await job.result()
    assert res is not None and res.success and res.value == 5


async def test_serve_logs_lifecycle_with_burst_reason(redis, make_app, caplog):
    app = make_app("cli_log", concurrency=1, poll_block_ms=50)

    with caplog.at_level(logging.INFO, logger="ardiq"):
        await asyncio.wait_for(serve(app, burst=True, quiet=True), timeout=15)

    messages = [r.message for r in caplog.records if r.name == "ardiq"]
    assert any(
        m.startswith("worker starting") and f"worker_id={app.worker_id}" in m
        for m in messages
    )
    assert f"worker stopped worker_id={app.worker_id} reason=burst" in messages
