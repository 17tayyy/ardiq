"""Command-line interface: `ardiq run module:app`."""

from __future__ import annotations

import argparse
import asyncio
import atexit
import contextlib
import importlib
import logging
import os
import signal
import subprocess
import sys
import time
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
    run.add_argument(
        "-w",
        "--workers",
        type=_positive,
        default=1,
        metavar="N",
        help="Run N worker processes instead of one (default: 1)",
    )
    return parser


def _positive(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be 1 or more")
    return number


POLL_CHILDREN_S = 0.2


def _supervise(args: argparse.Namespace) -> None:
    """Run the workers as child processes and stay until they are done.

    Each child is a plain `ardiq run` of its own, so it keeps the CLI's exit
    path — the one thing standing between a finished worker and a segfault on
    the way out.
    """
    child_argv = [sys.executable, "-m", "ardiq", "run", args.app, "--quiet"]
    if args.burst:
        child_argv.append("--burst")
    if args.verbose:
        child_argv.append("--verbose")

    if not args.quiet:
        from ardiq.banner import print_startup_banner

        print_startup_banner(
            import_string(args.app),
            app_path=args.app,
            burst=args.burst,
            workers=args.workers,
        )

    children = [subprocess.Popen(child_argv) for _ in range(args.workers)]
    logger.info(
        f"supervisor starting workers={args.workers} "
        f"pids={','.join(str(c.pid) for c in children)}"
    )

    stopping = False

    def stop_children() -> None:
        nonlocal stopping
        stopping = True
        for child in children:
            with contextlib.suppress(OSError):
                child.terminate()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(ValueError, OSError):
            signal.signal(sig, lambda *_: stop_children())

    failed = 0
    running = list(children)
    while running:
        time.sleep(POLL_CHILDREN_S)
        for child in list(running):
            code = child.poll()
            if code is None:
                continue
            running.remove(child)
            if code and not stopping:
                failed = code
                logger.error(f"worker died pid={child.pid} code={code}")
                stop_children()

    logger.info(f"supervisor stopped workers={args.workers} code={failed}")
    if failed:
        raise SystemExit(failed)


def _run(args: argparse.Namespace) -> None:
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    init_logging(args.verbose)  # surface the Rust core's logs too
    if args.workers > 1:
        _supervise(args)
        return
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


def _exit_without_finalizing() -> None:
    """Leave the process without tearing the interpreter down.

    The core's tokio threads outlive the worker, and a Python reference
    released from one of them while CPython finalizes segfaults for about 1% of
    workers under load. `atexit` handlers still run, so Sentry and coverage
    flush as usual.
    """
    atexit._run_exitfuncs()
    logging.shutdown()
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)


def console_main() -> None:
    """Entry point for the `ardiq` command. `main` stays importable and returns."""
    main()
    _exit_without_finalizing()
