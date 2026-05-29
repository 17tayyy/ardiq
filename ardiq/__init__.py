"""ArdiQ Python side.

The Rust core (`ardiq._core.ArdiqCore`) owns the loop and Redis I/O; this module
owns the wire format (msgpack) and the `execute` shim — the single interface the
core calls back into for every task. The `@task` decorator and enqueue client
will build on top of `REGISTRY` / `execute` later.
"""

import asyncio
import time
from collections.abc import Callable
from typing import Any

import msgpack

from ardiq._core import ArdiqCore

__all__ = ["REGISTRY", "ArdiqCore", "execute", "pack_task", "register"]

# Outcome codes expected by the Rust core (see ArdiqCore docs).
SUCCESS, FAILURE, RETRY = 0, 1, 2

# fn_name -> callable. Populated by `register` (and later by `@task`).
REGISTRY: dict[str, Callable[..., Any]] = {}


def register(name: str, fn: Callable[..., Any]) -> None:
    REGISTRY[name] = fn


def pack_task(
    fn_name: str,
    args: tuple = (),
    kwargs: dict | None = None,
    enqueue_time: int | None = None,
) -> bytes:
    return msgpack.packb(
        {
            "f": fn_name,
            "a": list(args),
            "k": kwargs or {},
            "t": enqueue_time if enqueue_time is not None else int(time.time() * 1000),
        }
    )


async def execute(task_id: str, payload: bytes, tries: int) -> tuple[int, bytes, int]:
    """Run one task for the core. Returns (outcome, result_bytes, retry_after_ms).

    The core hands back opaque bytes, so deciding success/failure and building the
    result envelope both live here.
    """
    data = msgpack.unpackb(payload, raw=False)
    fn = REGISTRY.get(data["f"])
    if fn is None:
        return FAILURE, _envelope(False, f"unknown task {data['f']!r}", tries), 0

    try:
        result = fn(*data["a"], **data["k"])
        if asyncio.iscoroutine(result):
            result = await result
    except Exception as exc:
        return FAILURE, _envelope(False, repr(exc), tries), 0

    return SUCCESS, _envelope(True, result, tries), 0


def _envelope(success: bool, result: Any, tries: int) -> bytes:
    return msgpack.packb({"s": success, "r": result, "t": tries})
