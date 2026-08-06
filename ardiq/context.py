"""The task a worker is currently running, readable from inside the task body."""

from __future__ import annotations

from contextvars import ContextVar

from ardiq.models import TaskContext

_current_task: ContextVar[TaskContext | None] = ContextVar(
    "ardiq_current_task", default=None
)


def current_task() -> TaskContext | None:
    """The task running right here, or `None` outside one.

    The worker sets it around every attempt, so a task — or anything it calls,
    including a sync task in its thread — can log its own id and try count
    without being handed them. `None` outside a worker, like
    `asyncio.current_task()`, so shared helpers can call it anywhere.

    ```python
    @app.task()
    async def charge(order_id: int) -> None:
        task = current_task()
        log.info("charging %s", order_id, extra={"task": task and task.task_id})
    ```
    """
    return _current_task.get()
