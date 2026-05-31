"""Command-line interface: `ardiq run module:app`."""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import logging
import signal
from typing import TYPE_CHECKING, Annotated

import typer

from ardiq import REGISTRY

if TYPE_CHECKING:
    from ardiq import Ardiq

logger = logging.getLogger("ardiq")

cli = typer.Typer(no_args_is_help=True, add_completion=False)


@cli.callback()
def _root() -> None:
    """ArdiQ — a Rust-powered distributed task queue."""


def import_string(path: str) -> Ardiq:
    """Load an Ardiq app from a 'module.sub:attr' path."""
    module_path, sep, attr = path.partition(":")
    if not sep or not module_path or not attr:
        raise ValueError(f"expected 'module:attr', got {path!r}")
    module = importlib.import_module(module_path)
    try:
        return getattr(module, attr)
    except AttributeError as exc:
        raise ValueError(f"{module_path!r} has no attribute {attr!r}") from exc


async def serve(app: Ardiq, burst: bool) -> None:
    """Run a worker until the queue drains (burst) or a signal stops it."""
    app.burst = burst
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError, ValueError, RuntimeError):
            loop.add_signal_handler(sig, app.stop)

    logger.info(
        "starting worker %s for %d task(s)%s",
        app.worker_id,
        len(REGISTRY),
        " [burst]" if burst else "",
    )
    try:
        await app.run()
    finally:
        logger.info("worker %s stopped", app.worker_id)


@cli.command(help="Run a worker")
def run(
    app: Annotated[str, typer.Argument(help="App path, e.g. 'myapp:app'")],
    burst: Annotated[
        bool, typer.Option("--burst", "-b", help="Exit once the queue drains")
    ] = False,
    verbose: Annotated[
        bool, typer.Option("--verbose", "-v", help="Use DEBUG-level logging")
    ] = False,
) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    worker = import_string(app)
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(serve(worker, burst))


def main(argv: list[str] | None = None) -> None:
    cli(args=argv)
