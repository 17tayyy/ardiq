"""The Ardiq app: owns the Rust core, the task registry, and the wire codec."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from ardiq._core import ArdiqCore
from ardiq.codec import _default_dumps, _default_loads
from ardiq.cron import _Schedule
from ardiq.models import TaskInfo, TaskResult
from ardiq.tasks import Job, Task

# Outcome codes for the Rust core's executor protocol.
SUCCESS, FAILURE, RETRY = 0, 1, 2
DEFAULT_MAX_RETRIES = 3

logger = logging.getLogger("ardiq")


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass(slots=True)
class _Registered:
    fn: Callable[..., Any]
    max_retries: int
    backoff_ms: int
    is_async: bool
    timeout: float | None  # seconds; None = no timeout


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
        cron_poll_s: float = 1.0,
        **core_kwargs: Any,
    ):
        self._dumps = serializer or _default_dumps
        self._loads = deserializer or _default_loads
        self._registry: dict[str, _Registered] = {}
        self._crons: dict[str, tuple[_Schedule, str | None]] = {}
        self._cron_poll_s = cron_poll_s
        config = {
            "redis_url": redis_url,
            "queue_name": queue_name,
            "priorities": priorities,
            **core_kwargs,
        }
        self._core = ArdiqCore({k: v for k, v in config.items() if v is not None})

    @property
    def worker_id(self) -> str:
        """This worker's unique id."""
        return self._core.worker_id

    @property
    def burst(self) -> bool:
        """Whether the worker exits once the queue drains."""
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
        """Register a function as a task. Returns a `Task` you can `.enqueue`."""

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

    def cron(
        self,
        spec: str | None = None,
        *,
        every: timedelta | float | None = None,
        name: str | None = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff_ms: int = 0,
        timeout: float | None = None,
        priority: str | None = None,
    ) -> Callable[[Callable[..., Any]], Task]:
        """Register a recurring task. Pass a 5-field cron `spec` (UTC) or an
        `every=` interval (timedelta or seconds). It fires while a worker runs."""
        schedule = _Schedule(every=every, cron=spec)

        def wrap(fn: Callable[..., Any]) -> Task:
            task_name = name or getattr(fn, "__name__", None)
            if task_name is None:
                raise TypeError("@cron needs an explicit name for this callable")
            self._registry[task_name] = _Registered(
                fn, max_retries, backoff_ms, asyncio.iscoroutinefunction(fn), timeout
            )
            self._crons[task_name] = (schedule, priority)
            return Task(self, task_name, fn, priority)

        return wrap

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

    async def _enqueue_cron(
        self, name: str, fire_ms: int, priority: str | None
    ) -> None:
        payload = self._pack(name, (), {})
        await self._core.enqueue(
            f"cron:{name}:{fire_ms}", payload, priority, 0, fire_ms, 0
        )

    async def _cron_scheduler(self) -> None:
        """Keep each cron's next occurrence staged in the delayed queue. The Rust
        producer promotes it when due; dedup makes re-staging a no-op."""
        while True:
            now = _now_ms()
            for cron_name, (schedule, priority) in self._crons.items():
                try:
                    await self._enqueue_cron(
                        cron_name, schedule.next_after(now), priority
                    )
                except Exception:
                    logger.exception("ardiq cron scheduling failed for %r", cron_name)
            await asyncio.sleep(self._cron_poll_s)

    def _pack(self, fn_name: str, args: tuple, kwargs: dict) -> bytes:
        return self._dumps({"f": fn_name, "a": list(args), "k": kwargs, "t": _now_ms()})

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
        """Run the worker loop until `stop()` (or the queue drains, in burst mode)."""
        # Cron makes no sense under burst (it drains and exits), so skip it there.
        if not self._crons or self.burst:
            await self._core.run(self._execute)
            return
        scheduler = asyncio.ensure_future(self._cron_scheduler())
        try:
            await self._core.run(self._execute)
        finally:
            scheduler.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await scheduler

    def stop(self) -> None:
        """Signal the worker loop to shut down."""
        self._core.stop()

    async def queue_size(self) -> int:
        """Number of tasks waiting in the queue (live streams + delayed)."""
        return await self._core.queue_size()

    async def result(
        self, task_id: str, timeout: float | None = None
    ) -> TaskResult | None:
        """Fetch a task's result. With `timeout` (seconds), wait for it to be
        stored, raising `TimeoutError` if it isn't in time; without, return the
        result now or `None` if it isn't ready."""
        if timeout is None:
            return self._unpack(await self._core.result(task_id))
        raw = await self._core.await_result(task_id, int(timeout * 1000))
        if raw is None:
            raise TimeoutError(f"no result for {task_id!r} within {timeout}s")

        return self._unpack(raw)

    async def status(self, task_id: str) -> str:
        """A task's status: 'queued', 'scheduled', 'running', 'complete', or 'not_found'."""
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
