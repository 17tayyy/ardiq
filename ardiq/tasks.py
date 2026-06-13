"""Task handles: `Task` (a registered task), `Job` (an enqueued one), and the
`_BoundTask` carrying one-off enqueue options."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

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
        """Dispatch the task with these args; returns a `Job` handle."""
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
        """Bind one-off enqueue options (delay, schedule, priority, id) for `.enqueue`."""
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
