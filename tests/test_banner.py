"""Startup banner tests."""

from datetime import timedelta
from io import StringIO

from rich.console import Console

from ardiq import Ardiq
from ardiq.banner import print_startup_banner


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

    buf = StringIO()
    print_startup_banner(
        app,
        app_path="myapp:app",
        burst=False,
        console=Console(file=buf, width=120, highlight=False),
    )
    out = buf.getvalue()

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

    buf = StringIO()
    print_startup_banner(
        app,
        app_path="app:worker",
        burst=True,
        console=Console(file=buf, width=120, highlight=False),
    )
    assert "burst" in buf.getvalue()


def test_startup_banner_lists_crons(make_app):
    app = make_app("banner-cron")

    @app.cron(every=timedelta(minutes=5))
    async def heartbeat():
        pass

    buf = StringIO()
    print_startup_banner(
        app,
        app_path="app:worker",
        burst=False,
        console=Console(file=buf, width=120, highlight=False),
    )
    out = buf.getvalue()
    assert "crons" in out.lower()
    assert "heartbeat" in out


def test_startup_banner_truncates_long_task_list(make_app):
    app = make_app("banner-many")

    for i in range(12):

        @app.task(name=f"task_{i}")
        async def _fn():
            pass

    buf = StringIO()
    print_startup_banner(
        app,
        app_path="app:worker",
        burst=False,
        console=Console(file=buf, width=120, highlight=False),
    )
    out = buf.getvalue()
    assert "(+4 more)" in out


def test_startup_banner_masks_redis_password():
    app = Ardiq(
        redis_url="redis://:secret@redis.example.com:6379/0",
        queue_name="banner-redis",
    )

    buf = StringIO()
    print_startup_banner(
        app,
        app_path="app:worker",
        burst=False,
        console=Console(file=buf, width=120, highlight=False),
    )
    out = buf.getvalue()
    assert "secret" not in out
    assert ":***@" in out
