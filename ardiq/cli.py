"""Command-line interface: `ardiq run module:app`."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import importlib
import logging
import signal
import sys
from typing import TYPE_CHECKING

from ardiq._core import init_logging

if TYPE_CHECKING:
    from ardiq import Ardiq

logger = logging.getLogger("ardiq")


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


async def serve(
    app: Ardiq,
    burst: bool,
    *,
    app_path: str = "",
    quiet: bool = False,
) -> None:
    """Run a worker until the queue drains (burst) or a signal stops it."""
    app.burst = burst
    stop_reason: str | None = None

    def _on_signal() -> None:
        nonlocal stop_reason
        stop_reason = "signal"
        app.stop()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError, ValueError, RuntimeError):
            loop.add_signal_handler(sig, _on_signal)

    if not quiet:
        from ardiq.banner import print_startup_banner

        print_startup_banner(app, app_path=app_path or "?", burst=burst)
        logger.info(f"worker starting worker_id={app.worker_id}")
    else:
        logger.info(
            f"worker starting worker_id={app.worker_id} queue={app.queue_name} "
            f"concurrency={app.concurrency} prefetch={app.prefetch} burst={burst} "
            f"tasks={len(app.tasks)} crons={len(app.crons)}"
        )
    try:
        await app.run()
    finally:
        reason = stop_reason or ("burst" if burst else "unknown")
        logger.info(f"worker stopped worker_id={app.worker_id} reason={reason}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ardiq",
        description="ArdiQ — a Rust-powered distributed task queue.",
    )
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    run = sub.add_parser("run", help="Run a worker", description="Run a worker")
    run.add_argument("app", help="App path, e.g. 'myapp:app'")
    run.add_argument(
        "-b", "--burst", action="store_true", help="Exit once the queue drains"
    )
    run.add_argument(
        "-v", "--verbose", action="store_true", help="Use DEBUG-level logging"
    )
    run.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Skip the startup banner (plain log line instead)",
    )
    return parser


def _run(args: argparse.Namespace) -> None:
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    init_logging(args.verbose)  # surface the Rust core's logs too
    worker = import_string(args.app)
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(serve(worker, args.burst, app_path=args.app, quiet=args.quiet))


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help(sys.stderr)
        raise SystemExit(1)
    _run(args)
