"""Startup banner tests."""

import subprocess
import sys
from datetime import timedelta
from io import StringIO

from ardiq import Ardiq
from ardiq.banner import print_startup_banner


def render(app, *, app_path="myapp:app", burst=False, **kwargs):
    buf = StringIO()
    print_startup_banner(app, app_path=app_path, burst=burst, file=buf, **kwargs)
    return buf.getvalue()


def test_config_defaults_come_from_core():
    app = Ardiq()
    assert app.redis_url == "redis://localhost:6379"
    assert app.queue_name == "default"
    assert app.priorities == ["default"]
    assert app.concurrency == 16
    assert app.prefetch == 32


def test_startup_banner_shows_config(make_app):
    app = make_app(
        "banner",
        concurrency=4,
        prefetch=8,
        priorities=["high", "default"],
    )

    @app.task()
    async def add(a, b):
        return a + b

    @app.task()
    async def mul(a, b):
        return a * b

    out = render(app)

    assert "ArdiQ worker" in out
    assert "myapp:app" in out
    assert app.worker_id in out
    assert "banner" in out
    assert "high, default" in out
    assert "4" in out
    assert "8" in out
    assert "continuous" in out
    assert "add" in out and "mul" in out


def test_startup_banner_burst_mode(make_app):
    app = make_app("banner-burst")

    @app.task()
    async def ping():
        return "pong"

    assert "burst" in render(app, app_path="app:worker", burst=True)


def test_startup_banner_lists_crons(make_app):
    app = make_app("banner-cron")

    @app.cron(every=timedelta(minutes=5))
    async def heartbeat():
        pass

    out = render(app, app_path="app:worker")
    assert "crons" in out.lower()
    assert "heartbeat" in out


def test_startup_banner_truncates_long_task_list(make_app):
    app = make_app("banner-many")

    for i in range(12):

        @app.task(name=f"task_{i}")
        async def _fn():
            pass

    assert "(+4 more)" in render(app, app_path="app:worker")


def test_startup_banner_masks_redis_password():
    app = Ardiq(
        redis_url="redis://:secret@redis.example.com:6379/0",
        queue_name="banner-redis",
    )

    out = render(app, app_path="app:worker")
    assert "secret" not in out
    assert ":***@" in out


def test_startup_banner_is_plain_when_not_a_tty(make_app):
    # StringIO has no isatty() returning True, so nothing should be coloured —
    # otherwise redirected worker logs fill up with escape sequences.
    assert "\033[" not in render(make_app("banner-plain"))


def test_startup_banner_rows_align_in_a_closed_box(make_app):
    lines = render(make_app("banner-box")).splitlines()
    assert len({len(line) for line in lines}) == 1, "box edges are ragged"

    # Values start at one column, whatever the key length.
    starts = {line.index(val) for line, val in _labelled_rows(lines)}
    assert len(starts) == 1, "value column is ragged"


def test_startup_banner_centres_title_and_subtitle(make_app):
    lines = render(make_app("banner-centre")).splitlines()

    for line, label in ((lines[0], "ArdiQ worker"), (lines[-1], "bytay.dev")):
        before, _, after = line.partition(label)
        # Border runs on both sides, within a character of each other.
        assert abs(len(before) - len(after)) <= 1, f"{label!r} is not centred"


def _labelled_rows(lines):
    for label, value in (("queue", "banner-box"), ("mode", "continuous")):
        line = next(ln for ln in lines if f" {label} " in ln)
        yield line, value


def test_worker_cli_does_not_import_rich_or_typer():
    # The banner used to be drawn with Rich, which cost ~3 MB of RSS in every
    # worker process. Guard the whole CLI path, not just the import.
    code = """
import io, sys
from ardiq import Ardiq
from ardiq.banner import print_startup_banner
import ardiq.cli

app = Ardiq(queue_name="import-guard")
print_startup_banner(app, app_path="x:app", burst=False, file=io.StringIO())
ardiq.cli.build_parser()
heavy = sorted({m.split(".")[0] for m in sys.modules} & {"rich", "typer", "click", "pygments"})
print(",".join(heavy))
"""
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    assert out.stdout.strip() == "", f"CLI path imported: {out.stdout.strip()}"
