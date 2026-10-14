"""The Rust core's log lines: colour only on a terminal, never under NO_COLOR.
Each case runs in a fresh process, since the subscriber installs once."""

import os
import pty
import subprocess
import sys

import pytest

WORKER = """
import asyncio, os
from ardiq import Ardiq
from ardiq._core import init_logging

init_logging(False)
app = Ardiq(
    redis_url=os.environ["ARDIQ_TEST_REDIS"], queue_name="core-logs", poll_block_ms=50
)
app.burst = True
asyncio.run(app.run())
"""


def _stderr_of_worker(redis_url: str, tty: bool, no_color: str | None) -> str:
    env = {k: v for k, v in os.environ.items() if k not in ("NO_COLOR", "RUST_LOG")}
    env["ARDIQ_TEST_REDIS"] = redis_url
    if no_color is not None:
        env["NO_COLOR"] = no_color
    if not tty:
        proc = subprocess.run(
            [sys.executable, "-c", WORKER], env=env, capture_output=True, timeout=60
        )
        assert proc.returncode == 0, proc.stderr.decode()
        return proc.stderr.decode()

    master, slave = pty.openpty()
    proc = subprocess.Popen(
        [sys.executable, "-c", WORKER], env=env, stdout=slave, stderr=slave
    )
    os.close(slave)
    out = b""
    while True:
        try:
            chunk = os.read(master, 4096)
        except OSError:  # EIO once the child closes its end
            break
        if not chunk:
            break
        out += chunk
    os.close(master)
    assert proc.wait(timeout=60) == 0, out.decode()
    return out.decode()


@pytest.mark.parametrize(
    ("tty", "no_color", "coloured"),
    [(False, None, False), (True, None, True), (True, "1", False), (True, "", True)],
    ids=["pipe", "terminal", "terminal-no-color", "terminal-empty-no-color"],
)
async def test_core_logs_colour_only_a_terminal(
    redis, make_app, tty, no_color, coloured
):
    err = _stderr_of_worker(make_app("core-logs").redis_url, tty, no_color)

    assert "ardiq worker started" in err
    assert ("\x1b[" in err) is coloured
