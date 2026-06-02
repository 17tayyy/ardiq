"""ArdiQ Python API: the Ardiq app, the @task decorator, and task handles.

The Rust core owns the loop and Redis I/O; an `Ardiq` app owns its task registry,
its wire codec, and the executor the core calls back into for every task.
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

__all__ = ["Ardiq", "Job", "Task", "TaskInfo", "TaskResult"]

# Outcome codes for the Rust core's executor protocol.
SUCCESS, FAILURE, RETRY = 0, 1, 2
DEFAULT_MAX_RETRIES = 3


def _now_ms() -> int:
    return int(time.time() * 1000)


def _default_dumps(obj: Any) -> bytes:
    return msgpack.packb(obj)


def _default_loads(data: bytes) -> Any:
    return msgpack.unpackb(data, raw=False)


@dataclass(slots=True)
class _Registered:
    fn: Callable[..., Any]
    max_retries: int
    backoff_ms: int
    is_async: bool
    timeout: float | None  # seconds; None = no timeout


class TaskResult(NamedTuple):
    """A decoded result envelope. On failure, `value` holds the error repr.

    Times are epoch ms: `enqueue_time` when enqueued, `start`/`finish` around
    execution.
    """

    success: bool
    value: Any
    tries: int
    enqueue_time: int = 0
    start: int = 0
    finish: int = 0

    @property
    def duration_ms(self) -> int:
        return self.finish - self.start


class TaskInfo(NamedTuple):
    """Snapshot of an unfinished task (queued, scheduled, or running)."""

    task_id: str
    fn_name: str
    args: tuple
    kwargs: dict
    enqueue_time: int
    tries: int
    status: str
    scheduled_at: int | None = None  # epoch ms if waiting in the delayed queue


@dataclass(frozen=True, slots=True)
class Job:
    """Handle to an enqueued task."""

    app: Ardiq
    id: str

    async def result(self, timeout: float | None = None) -> TaskResult | None:
        return await self.app.result(self.id, timeout=timeout)

    async def status(self) -> str:
        return await self.app.status(self.id)

    async def info(self) -> TaskInfo | None:
        return await self.app.info(self.id)


class Task:
    """A registered task. Call it to run inline, or `.enqueue` to dispatch."""

    def __init__(
        self, app: Ardiq, name: str, fn: Callable[..., Any], priority: str | None
    ):
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
    """App: owns the core, its task registry, and its wire codec."""

    def __init__(
        self,
        redis_url: str | None = None,
        queue_name: str = "default",
        priorities: list[str] | None = None,
        *,
        serializer: Callable[[Any], bytes] | None = None,
        deserializer: Callable[[bytes], Any] | None = None,
        **core_kwargs: Any,
    ):
        self._dumps = serializer or _default_dumps
        self._loads = deserializer or _default_loads
        self._registry: dict[str, _Registered] = {}
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

    @property
    def burst(self) -> bool:
        return self._core.burst

    @burst.setter
    def burst(self, value: bool) -> None:
        self._core.burst = value

    @property
    def tasks(self) -> list[str]:
        """Names of the registered tasks."""
        return list(self._registry)

    def task(
        self,
        fn: Callable[..., Any] | None = None,
        *,
        name: str | None = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff_ms: int = 0,
        timeout: float | None = None,
        priority: str | None = None,
    ) -> Any:
        def wrap(fn: Callable[..., Any]) -> Task:
            task_name = name or getattr(fn, "__name__", None)
            if task_name is None:
                raise TypeError("@task needs an explicit name for this callable")
            self._registry[task_name] = _Registered(
                fn,
                max_retries,
                backoff_ms,
                asyncio.iscoroutinefunction(fn),
                timeout,
            )
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
        payload = self._pack(task.name, args, kwargs)
        await self._core.enqueue(
            job_id, payload, priority or task.priority, delay_ms, schedule_ms, expire_ms
        )
        return Job(self, job_id)

    def _pack(self, fn_name: str, args: tuple, kwargs: dict) -> bytes:
        return self._dumps(
            {"f": fn_name, "a": list(args), "k": kwargs, "t": _now_ms()}
        )

    def _envelope(
        self, success: bool, result: Any, tries: int, enqueue_time: int, start: int
    ) -> bytes:
        return self._dumps(
            {
                "s": success,
                "r": result,
                "t": tries,
                "et": enqueue_time,
                "st": start,
                "ft": _now_ms(),
            }
        )

    def _unpack(self, raw: bytes | None) -> TaskResult | None:
        if raw is None:
            return None
        env = self._loads(raw)
        return TaskResult(
            env["s"],
            env["r"],
            env["t"],
            env.get("et", 0),
            env.get("st", 0),
            env.get("ft", 0),
        )

    async def _execute(
        self, task_id: str, payload: bytes, tries: int
    ) -> tuple[int, bytes, int]:
        """The core's per-task callback. Returns (outcome, result_bytes, retry_ms)."""
        data = self._loads(payload)
        enqueue_time = int(data.get("t", 0))
        start = _now_ms()
        reg = self._registry.get(data["f"])
        if reg is None:
            env = self._envelope(
                False, f"unknown task {data['f']!r}", tries, enqueue_time, start
            )
            return FAILURE, env, 0

        try:
            if reg.is_async:
                coro = reg.fn(*data["a"], **data["k"])
            else:
                coro = asyncio.to_thread(reg.fn, *data["a"], **data["k"])
            if reg.timeout is not None:
                result = await asyncio.wait_for(coro, reg.timeout)
            else:
                result = await coro
        except Exception as exc:
            if isinstance(exc, TimeoutError) and reg.timeout is not None:
                err = f"timed out after {reg.timeout}s"
            else:
                err = repr(exc)
            if tries <= reg.max_retries:
                return RETRY, b"", reg.backoff_ms  # 0 = core's default backoff
            return FAILURE, self._envelope(False, err, tries, enqueue_time, start), 0

        return SUCCESS, self._envelope(True, result, tries, enqueue_time, start), 0

    async def run(self) -> None:
        await self._core.run(self._execute)

    def stop(self) -> None:
        self._core.stop()

    async def queue_size(self) -> int:
        return await self._core.queue_size()

    async def result(
        self, task_id: str, timeout: float | None = None
    ) -> TaskResult | None:
        """Fetch a task's result. With `timeout` (seconds), wait for it to be
        stored, raising `TimeoutError` if it isn't in time; without, return the
        result now or `None` if it isn't ready."""
        if timeout is None:
            return self._unpack(await self._core.result(task_id))
        deadline = time.monotonic() + timeout
        while True:
            raw = await self._core.result(task_id)
            if raw is not None:
                return self._unpack(raw)
            if time.monotonic() >= deadline:
                raise TimeoutError(f"no result for {task_id!r} within {timeout}s")
            await asyncio.sleep(0.05)

    async def status(self, task_id: str) -> str:
        return await self._core.status(task_id)

    async def info(self, task_id: str) -> TaskInfo | None:
        """Metadata for an unfinished task, or `None` if it's finished/unknown
        (use `result` for finished tasks)."""
        payload, tries, scheduled_at = await self._core.task_info(task_id)
        if payload is None:
            return None
        data = self._loads(payload)
        return TaskInfo(
            task_id=task_id,
            fn_name=data["f"],
            args=tuple(data["a"]),
            kwargs=data["k"],
            enqueue_time=int(data["t"]),
            tries=tries,
            status=await self.status(task_id),
            scheduled_at=scheduled_at or None,
        )
