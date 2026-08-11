"""The Ardiq app: owns the Rust core, the task registry, and the wire codec."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
import time
import uuid
from collections.abc import AsyncIterator, Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, overload

from ardiq._core import ArdiqCore
from ardiq.codec import _default_dumps, _default_loads
from ardiq.context import _current_task
from ardiq.cron import _Schedule
from ardiq.exceptions import Retry
from ardiq.models import (
    ABORTED,
    ErrorContext,
    State,
    TaskContext,
    TaskInfo,
    TaskResult,
)
from ardiq.tasks import Job, PreparedTask, Task

# Outcome codes for the Rust core's executor protocol.
SUCCESS, FAILURE, RETRY = 0, 1, 2
DEFAULT_MAX_RETRIES = 3

ErrorHook = Callable[[ErrorContext], Any]

ABORT_WAIT_MS = 500

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
        worker_id: str | None = None,
        *,
        default_priority: str | None = None,
        serializer: Callable[[Any], bytes] | None = None,
        deserializer: Callable[[bytes], Any] | None = None,
        cron_poll_s: float = 1.0,
        **core_kwargs: Any,
    ):
        self._dumps = serializer or _default_dumps
        self._loads = deserializer or _default_loads
        self._registry: dict[str, _Registered] = {}
        self._crons: dict[str, tuple[_Schedule, str | None]] = {}
        self._running: dict[str, asyncio.Task] = {}  # in-flight, for abort
        self._lifespan: Callable[[], AsyncIterator[Any]] | None = None
        self._error_hooks: list[ErrorHook] = []
        self.state = State()
        self._cron_poll_s = cron_poll_s
        config = {
            "redis_url": redis_url,
            "queue_name": queue_name,
            "priorities": priorities,
            "worker_id": worker_id,
            **core_kwargs,
        }
        self._core = ArdiqCore({k: v for k, v in config.items() if v is not None})

        lanes = self._core.priorities
        self._lanes = frozenset(lanes)
        if default_priority is not None and default_priority not in self._lanes:
            raise ValueError(
                f"default_priority {default_priority!r} is not one of {lanes}"
            )
        # Without an explicit choice, the middle lane. Forgetting `priority=`
        # must not quietly demote work — demoted work still completes, so
        # nothing ever tells you it happened.
        self._default_priority = default_priority or lanes[len(lanes) // 2]

    def _check_priority(self, priority: str | None) -> None:
        """Refuse a lane no consumer reads.

        Writing to an unconfigured lane is worse than silent: the stream exists,
        so `status()` reports `queued` forever, while `queue_size()` reports 0 —
        the two things an operator would check lie in opposite directions.
        """
        if priority is not None and priority not in self._lanes:
            raise ValueError(
                f"priority {priority!r} is not one of {self._core.priorities} — "
                "no worker reads that lane"
            )

    @property
    def default_priority(self) -> str:
        """The lane a task lands in when no priority is given."""
        return self._default_priority

    @property
    def redis_url(self) -> str:
        """Resolved Redis URL (defaults applied by the core)."""
        return self._core.redis_url

    @property
    def queue_name(self) -> str:
        """Queue name."""
        return self._core.queue_name

    @property
    def priorities(self) -> list[str]:
        """Configured priorities, in the order passed (lowest priority first)."""
        return self._core.priorities

    @property
    def concurrency(self) -> int:
        """Maximum concurrent task executions."""
        return self._core.concurrency

    @property
    def prefetch(self) -> int:
        """Maximum tasks prefetched from Redis."""
        return self._core.prefetch

    @property
    def idle_timeout_ms(self) -> int:
        """Idle time before reclaiming in-flight tasks from a dead worker."""
        return self._core.idle_timeout_ms

    @property
    def poll_block_ms(self) -> int:
        """Redis stream read block timeout."""
        return self._core.poll_block_ms

    @property
    def result_ttl_ms(self) -> int:
        """How long task results are kept in Redis."""
        return self._core.result_ttl_ms

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

    @property
    def crons(self) -> list[str]:
        """Names of the registered cron tasks."""
        return list(self._crons)

    @overload
    def task[**P, R](self, fn: Callable[P, R], /) -> Task[P, R]: ...

    @overload
    def task[**P, R](
        self,
        fn: None = None,
        *,
        name: str | None = ...,
        max_retries: int = ...,
        backoff_ms: int = ...,
        timeout: float | None = ...,
        priority: str | None = ...,
    ) -> Callable[[Callable[P, R]], Task[P, R]]: ...

    def task[**P, R](
        self,
        fn: Callable[P, R] | None = None,
        *,
        name: str | None = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff_ms: int = 0,
        timeout: float | None = None,
        priority: str | None = None,
    ) -> Any:
        """Register a function as a task. Returns a `Task` you can `.enqueue`."""
        self._check_priority(priority)

        def wrap(fn: Callable[P, R]) -> Task[P, R]:
            task_name = self._register(
                name, fn, "task", max_retries, backoff_ms, timeout
            )
            return Task(self, task_name, fn, priority)

        return wrap(fn) if fn is not None else wrap

    def cron[**P, R](
        self,
        spec: str | None = None,
        *,
        every: timedelta | float | None = None,
        name: str | None = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff_ms: int = 0,
        timeout: float | None = None,
        priority: str | None = None,
    ) -> Callable[[Callable[P, R]], Task[P, R]]:
        """Register a recurring task. Pass a 5-field cron `spec` (UTC) or an
        `every=` interval (timedelta or seconds). It fires while a worker runs."""
        schedule = _Schedule(every=every, cron=spec)
        self._check_priority(priority)

        def wrap(fn: Callable[P, R]) -> Task[P, R]:
            task_name = self._register(
                name, fn, "cron", max_retries, backoff_ms, timeout
            )
            self._crons[task_name] = (schedule, priority)
            return Task(self, task_name, fn, priority)

        return wrap

    def _register(
        self,
        name: str | None,
        fn: Callable[..., Any],
        decorator: str,
        max_retries: int,
        backoff_ms: int,
        timeout: float | None,
    ) -> str:
        """Put a task in the registry, refusing to shadow one already there."""
        task_name = name or getattr(fn, "__name__", None)
        if task_name is None:
            raise TypeError(f"@{decorator} needs an explicit name for this callable")
        existing = self._registry.get(task_name)
        if existing is not None:
            owner = getattr(existing.fn, "__module__", "?")
            raise ValueError(
                f"task {task_name!r} is already registered by {owner} — two tasks "
                "cannot share a name, or one silently replaces the other. Rename "
                "it, or give one an explicit name= in the decorator."
            )
        self._registry[task_name] = _Registered(
            fn, max_retries, backoff_ms, asyncio.iscoroutinefunction(fn), timeout
        )
        return task_name

    def lifespan(
        self, fn: Callable[[], AsyncIterator[Any]]
    ) -> Callable[[], AsyncIterator[Any]]:
        """Register startup/shutdown for the worker: an async generator with one
        `yield`. Set up before it, tear down after; yield a mapping to put its
        entries on `app.state`. Runs inside `run()`, so enqueue-only processes
        never pay for it."""
        if not inspect.isasyncgenfunction(fn):
            raise TypeError("@lifespan needs an async generator function (one yield)")
        self._lifespan = fn
        return fn

    @contextlib.asynccontextmanager
    async def _lifespan_scope(self) -> AsyncIterator[None]:
        if self._lifespan is None:
            yield
            return
        async with contextlib.asynccontextmanager(self._lifespan)() as deps:
            if deps is not None:
                if not isinstance(deps, Mapping):
                    raise TypeError("@lifespan must yield a mapping or nothing")
                for key, value in deps.items():
                    setattr(self.state, key, value)
            logger.info(f"lifespan started worker={self.worker_id}")
            try:
                yield
            finally:
                logger.info(f"lifespan stopping worker={self.worker_id}")

    def on_error(self, fn: ErrorHook) -> ErrorHook:
        """Register a hook run whenever an attempt goes wrong, before ArdiQ
        decides between retrying and failing. This is where a reporter like
        Sentry goes.

        The hook takes an `ErrorContext` and may be sync or async; register as
        many as you like and all of them run. One that raises is logged and
        never changes the task's outcome. It fires on timeouts and on every
        retry, but not on abort. Keep it quick — it runs on the worker's loop.
        """
        self._error_hooks.append(fn)
        return fn

    async def _fire_error_hooks(self, ctx: ErrorContext) -> None:
        for hook in self._error_hooks:
            try:
                outcome = hook(ctx)
                if inspect.isawaitable(outcome):
                    await outcome
            except Exception:
                logger.exception("ardiq on_error hook failed for %r", ctx.name)

    def ref(self, name: str, *, priority: str | None = None) -> Task[..., Any]:
        """A handle to a task registered somewhere else — same `.enqueue` and
        `.options` as a local task, but no function to call. Use it to reach a
        worker's tasks without importing (or registering) them here."""
        return Task(self, name, None, priority)

    async def send(self, name: str, *args: Any, **kwargs: Any) -> Job:
        """Enqueue a task by name, with no local registration. Shorthand for
        `ref(name).enqueue(...)`; use `ref` when you need enqueue options.

        Nothing checks the name here — an unknown one fails on the worker.
        """
        return await self._enqueue(name, args, kwargs)

    async def enqueue_many(self, tasks: Iterable[PreparedTask]) -> list[Job]:
        """Send tasks built with `.prepare()` in one round trip, not one each.

        Returns their `Job`s in the order given. Lanes are validated across the
        whole batch first, so a bad `priority` fails the call instead of leaving
        half of it enqueued.
        """
        prepared = list(tasks)
        for task in prepared:
            self._check_priority(task.priority)

        items, jobs = [], []
        for task in prepared:
            job_id = task.task_id or uuid.uuid4().hex
            items.append(
                (
                    job_id,
                    self._pack(task.name, task.args, task.kwargs),
                    task.priority or self._default_priority,
                    task.delay_ms,
                    task.schedule_ms,
                    task.expire_ms,
                )
            )
            jobs.append(Job(self, job_id))

        if items:
            await self._core.enqueue_many(items)
        return jobs

    async def _enqueue(
        self,
        name: str,
        args: tuple,
        kwargs: dict,
        *,
        task_id: str | None = None,
        priority: str | None = None,
        delay_ms: int = 0,
        schedule_ms: int = 0,
        expire_ms: int = 0,
    ) -> Job:
        self._check_priority(priority)
        job_id = task_id or uuid.uuid4().hex
        payload = self._pack(name, args, kwargs)
        await self._core.enqueue(
            job_id,
            payload,
            priority or self._default_priority,
            delay_ms,
            schedule_ms,
            expire_ms,
        )
        return Job(self, job_id)

    async def _enqueue_cron(
        self, name: str, fire_ms: int, priority: str | None
    ) -> None:
        # Deterministic id: re-staging the same occurrence is a no-op in Redis.
        await self._enqueue(
            name,
            (),
            {},
            task_id=f"cron:{name}:{fire_ms}",
            priority=priority,
            schedule_ms=fire_ms,
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

    async def _abort_watcher(self) -> None:
        """Cancel in-flight tasks that someone asked to abort. The core keeps one
        subscription per queue and hands ids over as they arrive."""
        while True:
            try:
                task_id = await self._core.next_abort(ABORT_WAIT_MS)
            except Exception:
                logger.exception("ardiq abort watcher failed")
                await asyncio.sleep(1.0)
                continue
            running = self._running.get(task_id) if task_id else None
            if running is not None and not running.done():
                running.cancel()

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
        self, task_id: str, payload: bytes, tries: int, aborted: bool = False
    ) -> tuple[int, bytes, int]:
        """The core's per-task callback. Returns (outcome, result_bytes, retry_ms)."""
        data = self._loads(payload)
        task_name = data["f"]
        worker_id = self.worker_id
        enqueue_time = int(data.get("t", 0))
        start = _now_ms()

        if aborted:
            logger.info(
                f"task aborted id={task_id} name={task_name!r} worker={worker_id} "
                f"try={tries} duration_ms=0"
            )
            env = self._envelope(False, ABORTED, tries, enqueue_time, start)
            return FAILURE, env, 0

        reg = self._registry.get(task_name)
        if reg is None:
            err = f"unknown task {task_name!r}"
            logger.error(
                f"task unknown id={task_id} name={task_name!r} worker={worker_id} try={tries}"
            )
            # No exception was raised here, but a worker that doesn't know a task
            # is a deployment mismatch — exactly what a reporter wants to see.
            await self._fire_error_hooks(
                ErrorContext(task_name, task_id, LookupError(err), tries, False)
            )
            env = self._envelope(False, err, tries, enqueue_time, start)
            return FAILURE, env, 0

        logger.debug(
            f"task started id={task_id} name={task_name!r} worker={worker_id} try={tries}"
        )

        current = asyncio.current_task()
        if current is not None:
            self._running[task_id] = current
        token = _current_task.set(TaskContext(task_id, task_name, tries))

        result = None
        error: Exception | None = None
        try:
            if reg.is_async:
                coro = reg.fn(*data["a"], **data["k"])
            else:
                coro = asyncio.to_thread(reg.fn, *data["a"], **data["k"])
            if reg.timeout is not None:
                result = await asyncio.wait_for(coro, reg.timeout)
            else:
                result = await coro
        except asyncio.CancelledError:
            if current is not None:
                current.uncancel()  # we handled it; let the task finish normally
            duration_ms = _now_ms() - start
            logger.warning(
                f"task aborted id={task_id} name={task_name!r} worker={worker_id} "
                f"try={tries} duration_ms={duration_ms}"
            )
            env = self._envelope(False, ABORTED, tries, enqueue_time, start)
            return FAILURE, env, 0
        except Exception as exc:
            error = exc
        finally:
            _current_task.reset(token)
            # Deregistered before the hooks run below: an abort landing while a
            # hook awaits would otherwise cancel us on the way out.
            self._running.pop(task_id, None)

        duration_ms = _now_ms() - start

        if error is not None:
            manual = isinstance(error, Retry)  # the task asked for this
            if isinstance(error, TimeoutError) and reg.timeout is not None:
                err = f"timed out after {reg.timeout}s"
            else:
                err = repr(error)

            if tries <= reg.max_retries:
                retry_ms = reg.backoff_ms
                if isinstance(error, Retry) and error.delay_ms is not None:
                    retry_ms = error.delay_ms
                # A retry the task asked for is control flow, not a fault: it is
                # logged quieter and kept out of the error hooks.
                log = logger.info if manual else logger.warning
                log(
                    f"task retry scheduled id={task_id} name={task_name!r} worker={worker_id} "
                    f"try={tries} delay_ms={retry_ms or tries * tries * 1000} error={err}"
                )
                if not manual:
                    await self._fire_error_hooks(
                        ErrorContext(task_name, task_id, error, tries, True)
                    )
                return RETRY, b"", retry_ms  # 0 = core's default backoff

            logger.error(
                f"task failed id={task_id} name={task_name!r} worker={worker_id} "
                f"try={tries} duration_ms={duration_ms} error={err}"
            )
            await self._fire_error_hooks(
                ErrorContext(task_name, task_id, error, tries, False)
            )
            return FAILURE, self._envelope(False, err, tries, enqueue_time, start), 0

        logger.debug(
            f"task succeeded id={task_id} name={task_name!r} worker={worker_id} "
            f"try={tries} duration_ms={duration_ms}"
        )
        return SUCCESS, self._envelope(True, result, tries, enqueue_time, start), 0

    async def run(self) -> None:
        """Run the worker loop until `stop()` (or the queue drains, in burst mode)."""
        async with self._lifespan_scope():
            # Cron and abort watching need a worker that sticks around, so burst
            # (which drains and exits) skips both. Aborts are still honored there:
            # the core checks for one before handing a task over.
            if self.burst:
                await self._core.run(self._execute)
                return
            background = [asyncio.ensure_future(self._abort_watcher())]
            if self._crons:
                background.append(asyncio.ensure_future(self._cron_scheduler()))
            try:
                await self._core.run(self._execute)
            finally:
                for task in background:
                    task.cancel()
                for task in background:
                    with contextlib.suppress(asyncio.CancelledError):
                        await task

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

    async def abort(self, task_id: str) -> bool:
        """Abort a task: stop it if a worker is running it, and make sure it never
        starts otherwise. Returns `False` if it already finished or is unknown.

        The task ends as a failed `TaskResult` with `aborted` set. A task waiting
        on a delay is finalized immediately; one already queued for pickup is
        dropped when a worker reaches it. Sync tasks can't be interrupted
        mid-call — the worker stops waiting, but the thread runs to completion.
        """
        env = self._envelope(False, ABORTED, 0, 0, _now_ms())
        return await self._core.abort(task_id, env)

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
