"""Public data types returned to callers: task results and live task info."""

from __future__ import annotations

from typing import Any, NamedTuple


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
        """Execution time in ms (`finish - start`)."""
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
