"""CLI: app loading and `ardiq run --burst`."""

import asyncio
import os

import pytest
from typer.testing import CliRunner

from ardiq.cli import cli, import_string, serve

runner = CliRunner()


def test_cli_help_lists_run():
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "run" in result.stdout


def test_cli_run_requires_app():
    result = runner.invoke(cli, ["run"])
    assert result.exit_code != 0


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
