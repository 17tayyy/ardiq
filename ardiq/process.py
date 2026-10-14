"""Run a sync task in a child process of its own: `@app.task(executor="process")`.

Each run spawns a fresh interpreter that imports the task's module, calls the
function and exits, so a timeout or an abort can stop it and its memory goes
back to the system. Log records it emits are sent to the worker and handled
there, as if the task had run in a thread.
"""

from __future__ import annotations

import asyncio
import copy
import importlib
import logging
import multiprocessing
import os
import pickle
import threading
import traceback
from collections.abc import Callable
from multiprocessing.connection import Connection
from typing import Any

from ardiq.context import _current_task
from ardiq.models import TaskContext

_SPAWN = multiprocessing.get_context("spawn")
KILL_AFTER_S = 5.0  # after terminate(), before kill()


def locate(fn: Callable[..., Any]) -> tuple[str, str]:
    """Where a child process imports `fn` from. Refused at registration if it
    is not a module-level function."""
    module = getattr(fn, "__module__", None)
    qualname = getattr(fn, "__qualname__", "")
    if not module or not qualname or "<" in qualname:
        raise TypeError(
            f'executor="process" needs a module-level function, got {fn!r}: '
            "the child process imports it by name"
        )
    return module, qualname


async def run_in_process(
    target: tuple[str, str], args: list, kwargs: dict, context: TaskContext
) -> Any:
    reader, writer = _SPAWN.Pipe(duplex=False)
    proc = _SPAWN.Process(
        target=_child,
        args=(
            writer,
            *target,
            args,
            kwargs,
            context,
            logging.getLogger().getEffectiveLevel(),
        ),
        daemon=True,
    )
    proc.start()
    writer.close()  # the child holds the only write end: its exit means EOF
    receiving = asyncio.get_running_loop().run_in_executor(None, _receive, reader)
    try:
        kind, value = await asyncio.shield(receiving)
    except asyncio.CancelledError:  # a timeout or an abort
        proc.terminate()
        try:
            await asyncio.wait_for(asyncio.shield(receiving), KILL_AFTER_S)
        except TimeoutError:
            proc.kill()
            await receiving
        raise
    finally:
        await asyncio.to_thread(proc.join)
        reader.close()
    if kind == "result":
        return value
    if kind == "error":
        raise value
    raise ChildProcessError(f"the task's process exited with code {proc.exitcode}")


def _receive(reader: Connection) -> tuple[str, Any]:
    while True:
        try:
            kind, value = reader.recv()
        except EOFError:
            return "gone", None
        if kind != "log":
            return kind, value
        logger = logging.getLogger(value.name)
        if logger.isEnabledFor(value.levelno):
            logger.handle(value)


def _child(
    writer: Connection,
    module: str,
    qualname: str,
    args: list,
    kwargs: dict,
    context: TaskContext,
    level: int,
) -> None:
    _exit_with_parent()
    lock = threading.Lock()

    def send(message: tuple[str, Any]) -> None:
        with lock:  # the task may log from threads of its own
            writer.send(message)

    root = logging.getLogger()
    root.handlers[:] = [_PipeHandler(send)]
    root.setLevel(level)
    _current_task.set(context)
    try:
        result = _resolve(module, qualname)(*args, **kwargs)
    except BaseException as exc:
        send(("error", _portable(exc)))
    else:
        try:
            send(("result", result))
        except Exception as exc:  # it cannot be pickled
            send(("error", TypeError(f"the task's result cannot be sent back: {exc}")))
    finally:
        writer.close()


class _PipeHandler(logging.Handler):
    """Sends each record to the worker, formatted down to what pickles."""

    def __init__(self, send: Callable[[tuple[str, Any]], None]):
        super().__init__()
        self._send = send

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)  # folds in the exception's traceback
            record = copy.copy(record)
            record.msg, record.message, record.args = msg, msg, None
            record.exc_info = record.exc_text = record.stack_info = None
            self._send(("log", record))
        except Exception:
            self.handleError(record)


def _resolve(module: str, qualname: str) -> Callable[..., Any]:
    from ardiq.tasks import Task

    target: Any = importlib.import_module(module)
    for part in qualname.split("."):
        target = getattr(target, part)
    # The decorator rebinds the module's name to the Task.
    return target.fn if isinstance(target, Task) else target


def _portable(exc: BaseException) -> BaseException:
    exc.add_note(
        "Traceback in the task's process:\n" + "".join(traceback.format_exception(exc))
    )
    try:
        pickle.loads(pickle.dumps(exc))
    except Exception:
        return RuntimeError(f"{type(exc).__name__}: {exc}")
    return exc


def _exit_with_parent() -> None:
    # A worker that dies hard (kill -9, a second Ctrl-C) must not leave a 1 GB
    # task running on its own.
    parent = multiprocessing.parent_process()
    if parent is None:
        return

    def watch() -> None:
        parent.join()
        os._exit(1)

    threading.Thread(target=watch, daemon=True).start()
