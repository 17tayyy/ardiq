"""ArdiQ Python API: the @task decorator, an enqueue client, and the execute shim.

The Rust core owns the loop and Redis I/O; this module owns the msgpack wire
format, the task registry, and `execute` (the single callback the core invokes).
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, NamedTuple

import msgpack

from ardiq._core import ArdiqCore

__all__ = [
    "DEFAULT_MAX_RETRIES",
    "REGISTRY",
    "Ardiq",
    "ArdiqCore",
    "Job",
    "Task",
    "TaskResult",
    "execute",
    "pack_task",
    "register",
    "unpack_result",
]

# Outcome codes expected by the Rust core (see ArdiqCore docs).
SUCCESS, FAILURE, RETRY = 0, 1, 2
DEFAULT_MAX_RETRIES = 3


@dataclass(slots=True)
class _Registered:
    fn: Callable[..., Any]
    max_retries: int
    backoff_ms: int


# name -> registered fn + retry policy. Populated by `register` / `@app.task`.
REGISTRY: dict[str, _Registered] = {}


def register(
    name: str,
    fn: Callable[..., Any],
    *,
    max_retries: int = 0,
    backoff_ms: int = 0,
) -> None:
    """Low-level registration. Defaults to no retries; `@app.task` adds them."""
    REGISTRY[name] = _Registered(fn, max_retries, backoff_ms)


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
    """Run one task. Returns (outcome, result_bytes, retry_after_ms)."""
    data = msgpack.unpackb(payload, raw=False)
    reg = REGISTRY.get(data["f"])
    if reg is None:
        return FAILURE, _envelope(False, f"unknown task {data['f']!r}", tries), 0

    try:
        result = reg.fn(*data["a"], **data["k"])
        if asyncio.iscoroutine(result):
            result = await result
    except Exception as exc:
        if tries <= reg.max_retries:
            return RETRY, b"", reg.backoff_ms  # 0 = core's default backoff
        return FAILURE, _envelope(False, repr(exc), tries), 0

    return SUCCESS, _envelope(True, result, tries), 0


def _envelope(success: bool, result: Any, tries: int) -> bytes:
    return msgpack.packb({"s": success, "r": result, "t": tries})


class TaskResult(NamedTuple):
    """A decoded result envelope. On failure, `value` holds the error repr."""

    success: bool
    value: Any
    tries: int


def unpack_result(raw: bytes | None) -> TaskResult | None:
    """Decode ArdiqCore.result bytes into a TaskResult (None passes through)."""
    if raw is None:
        return None
    env = msgpack.unpackb(raw, raw=False)
    return TaskResult(env["s"], env["r"], env["t"])


@dataclass(frozen=True, slots=True)
class Job:
    """Handle to an enqueued task."""

    app: Ardiq
    id: str

    async def result(self) -> TaskResult | None:
        return await self.app.result(self.id)

    async def status(self) -> str:
        return await self.app.status(self.id)


class Task:
    """A registered task. Call it to run inline, or `.enqueue` to dispatch."""

    def __init__(self, app: Ardiq, name: str, fn: Callable[..., Any], priority: str | None):
        self.app = app
        self.name = name
        self.fn = fn
        self.priority = priority

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.fn(*args, **kwargs)

    async def enqueue(self, *args: Any, **kwargs: Any) -> Job:
        return await self.app._enqueue(self, args, kwargs)

    def options(
        self,
        *,
        task_id: str | None = None,
        priority: str | None = None,
        delay_ms: int = 0,
        schedule_ms: int = 0,
        expire_ms: int = 0,
    ) -> _BoundTask:
        return _BoundTask(self, task_id, priority, delay_ms, schedule_ms, expire_ms)


@dataclass(frozen=True, slots=True)
class _BoundTask:
    """A task plus enqueue options, kept off `enqueue(*args, **kwargs)`."""

    task: Task
    task_id: str | None
    priority: str | None
    delay_ms: int
    schedule_ms: int
    expire_ms: int

    async def enqueue(self, *args: Any, **kwargs: Any) -> Job:
        return await self.task.app._enqueue(
            self.task,
            args,
            kwargs,
            task_id=self.task_id,
            priority=self.priority,
            delay_ms=self.delay_ms,
            schedule_ms=self.schedule_ms,
            expire_ms=self.expire_ms,
        )


class Ardiq:
    """App: holds the core, the @task decorator, and the enqueue client."""

    def __init__(
        self,
        redis_url: str | None = None,
        queue_name: str = "default",
        priorities: list[str] | None = None,
        **core_kwargs: Any,
    ):
        config = {
            "redis_url": redis_url,
            "queue_name": queue_name,
            "priorities": priorities,
            **core_kwargs,
        }
        self._core = ArdiqCore({k: v for k, v in config.items() if v is not None})

    @property
    def worker_id(self) -> str:
        return self._core.worker_id

    def task(
        self,
        fn: Callable[..., Any] | None = None,
        *,
        name: str | None = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff_ms: int = 0,
        priority: str | None = None,
    ) -> Any:
        def wrap(fn: Callable[..., Any]) -> Task:
            task_name = name or getattr(fn, "__name__", None)
            if task_name is None:
                raise TypeError("@task needs an explicit name for this callable")
            register(task_name, fn, max_retries=max_retries, backoff_ms=backoff_ms)
            return Task(self, task_name, fn, priority)

        return wrap(fn) if fn is not None else wrap

    async def _enqueue(
        self,
        task: Task,
        args: tuple,
        kwargs: dict,
        *,
        task_id: str | None = None,
        priority: str | None = None,
        delay_ms: int = 0,
        schedule_ms: int = 0,
        expire_ms: int = 0,
    ) -> Job:
        job_id = task_id or uuid.uuid4().hex
        payload = pack_task(task.name, args, kwargs)
        await self._core.enqueue(
            job_id, payload, priority or task.priority, delay_ms, schedule_ms, expire_ms
        )
        return Job(self, job_id)

    async def run(self) -> None:
        await self._core.run(execute)

    def stop(self) -> None:
        self._core.stop()

    async def queue_size(self) -> int:
        return await self._core.queue_size()

    async def result(self, task_id: str) -> TaskResult | None:
        return unpack_result(await self._core.result(task_id))

    async def status(self, task_id: str) -> str:
        return await self._core.status(task_id)
