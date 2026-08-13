"""Task handles: `Task` (a registered task), `Job` (an enqueued one), and the
`_BoundTask` carrying one-off enqueue options."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ardiq.models import TaskInfo, TaskResult

if TYPE_CHECKING:
    from ardiq.app import Ardiq


@dataclass(frozen=True, slots=True)
class Job:
    """Handle to an enqueued task."""

    app: Ardiq
    id: str

    async def result(self, timeout: float | None = None) -> TaskResult | None:
        """The task's result; with `timeout` (s), wait for it. See `Ardiq.result`."""
        return await self.app.result(self.id, timeout=timeout)

    async def status(self) -> str:
        """The task's current status. See `Ardiq.status`."""
        return await self.app.status(self.id)

    async def info(self) -> TaskInfo | None:
        """Metadata while the task is unfinished, else `None`. See `Ardiq.info`."""
        return await self.app.info(self.id)

    async def abort(self) -> bool:
        """Abort the task; `False` if it already finished. See `Ardiq.abort`."""
        return await self.app.abort(self.id)


class Task[**P, R]:
    """A registered task. Call it to run inline, or `.enqueue` to dispatch.

    Generic over the decorated function's signature, so `.enqueue(...)` is
    checked against it — a wrong argument is a type error, not a worker-side
    failure discovered in production.

    `Ardiq.ref` builds one with no local function — a handle to a task that
    lives in another process, enqueueable but not callable. Its parameters are
    unknown, so it types as `Task[..., Any]` and nothing is checked.
    """

    def __init__(
        self,
        app: Ardiq,
        name: str,
        fn: Callable[P, R] | None,
        priority: str | None,
        unique: bool = False,
    ):
        self.app = app
        self.name = name
        self.fn = fn
        self.priority = priority
        self.unique = unique

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        if self.fn is None:
            raise TypeError(f"task {self.name!r} is a reference, not a local function")
        return self.fn(*args, **kwargs)

    async def enqueue(self, *args: P.args, **kwargs: P.kwargs) -> Job:
        """Dispatch the task with these args; returns a `Job` handle."""
        return await self.app._enqueue(
            self.name, args, kwargs, priority=self.priority, unique=self.unique
        )

    def prepare(self, *args: P.args, **kwargs: P.kwargs) -> PreparedTask:
        """The call `.enqueue` would make, without making it — for `enqueue_many`."""
        return PreparedTask(
            self.name, args, kwargs, None, self.priority, 0, 0, 0, self.unique
        )

    def options(
        self,
        *,
        task_id: str | None = None,
        priority: str | None = None,
        delay_ms: int = 0,
        schedule_ms: int = 0,
        expire_ms: int = 0,
        unique: bool | None = None,
    ) -> _BoundTask[P, R]:
        """Bind one-off enqueue options (delay, schedule, priority, id, unique)
        for `.enqueue`."""
        return _BoundTask(
            self, task_id, priority, delay_ms, schedule_ms, expire_ms, unique
        )


@dataclass(frozen=True, slots=True)
class _BoundTask[**P, R]:
    """A task plus enqueue options, kept off `enqueue(*args, **kwargs)`."""

    task: Task[P, R]
    task_id: str | None
    priority: str | None
    delay_ms: int
    schedule_ms: int
    expire_ms: int
    unique: bool | None = None

    @property
    def _unique(self) -> bool:
        return self.task.unique if self.unique is None else self.unique

    async def enqueue(self, *args: P.args, **kwargs: P.kwargs) -> Job:
        return await self.task.app._enqueue(
            self.task.name,
            args,
            kwargs,
            task_id=self.task_id,
            priority=self.priority or self.task.priority,
            delay_ms=self.delay_ms,
            schedule_ms=self.schedule_ms,
            expire_ms=self.expire_ms,
            unique=self._unique,
        )

    def prepare(self, *args: P.args, **kwargs: P.kwargs) -> PreparedTask:
        """The call `.enqueue` would make, without making it — for `enqueue_many`."""
        return PreparedTask(
            self.task.name,
            args,
            kwargs,
            self.task_id,
            self.priority or self.task.priority,
            self.delay_ms,
            self.schedule_ms,
            self.expire_ms,
            self._unique,
        )


@dataclass(frozen=True, slots=True)
class PreparedTask:
    """One enqueue, held back so `Ardiq.enqueue_many` can send a batch at once.

    Built by `Task.prepare` / `Task.options(...).prepare`, which check the
    arguments against the task's signature exactly as `.enqueue` does.
    """

    name: str
    args: tuple
    kwargs: dict
    task_id: str | None
    priority: str | None
    delay_ms: int
    schedule_ms: int
    expire_ms: int
    unique: bool = False
