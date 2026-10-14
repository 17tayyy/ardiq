"""Tasks for the process executor tests. They are module-level so the child
process can import them by name."""

import logging
import os
import pathlib
import threading
import time

from ardiq import current_task
from ardiq.exceptions import Retry

log = logging.getLogger("process_tasks")


def add(a: int, b: int) -> tuple[int, int]:
    return a + b, os.getpid()


def boom() -> None:
    raise ValueError("broke in the child")


def again() -> None:
    raise Retry("not yet", delay_ms=10)


def hang(pid_file: str) -> None:
    pathlib.Path(pid_file).write_text(str(os.getpid()))
    time.sleep(60)


def chatty() -> int:
    log.info("hello from the child")
    log.debug("too quiet to cross")
    return os.getpid()


def whoami() -> tuple[str, int]:
    task = current_task()
    assert task is not None
    return task.task_id, task.tries


def crash() -> None:
    os._exit(3)


def unpicklable() -> threading.Lock:
    return threading.Lock()
