"""Public data types returned to callers: task results and live task info."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, NamedTuple

# The `value` an aborted task reports, in an otherwise ordinary failure envelope.
ABORTED = "aborted"


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

    @property
    def aborted(self) -> bool:
        """Whether the task was aborted instead of failing on its own."""
        return not self.success and self.value == ABORTED


class TaskContext(NamedTuple):
    """The task being executed, as seen from inside it (`ardiq.current_task()`)."""

    task_id: str
    name: str
    tries: int


class EnqueueContext(NamedTuple):
    """What an `@app.on_enqueue` hook is handed for each task being enqueued.

    Add entries to `headers` to send them along with the task; a worker's
    middleware reads them back from `ExecutionContext.headers`.
    """

    task_id: str
    name: str
    args: tuple
    kwargs: dict
    headers: dict[str, Any]


class ExecutionContext(NamedTuple):
    """What an `@app.middleware` is handed for each attempt it wraps."""

    task_id: str
    name: str
    tries: int
    args: tuple
    kwargs: dict
    headers: dict[str, Any]


class ErrorContext(NamedTuple):
    """What an `@app.on_error` hook is handed when an attempt goes wrong.

    `tries` is the attempt that just failed (1-based); `will_retry` says whether
    another one is coming.
    """

    name: str
    task_id: str
    exc: BaseException
    tries: int
    will_retry: bool


class State(SimpleNamespace):
    """Worker-scoped resources, set up by an `@app.lifespan` hook."""

    def __getattr__(self, name: str) -> Any:
        raise AttributeError(
            f"app.state has no {name!r} — set it from an @app.lifespan hook, "
            "which only runs inside app.run()"
        )


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


class DeadLetter(NamedTuple):
    """A task that failed for good, kept in the dead letter queue for replay.

    `error` is the failure as its result reports it; times are epoch ms.
    """

    task_id: str
    fn_name: str
    args: tuple
    kwargs: dict
    priority: str
    error: str
    tries: int
    enqueue_time: int
    failed_at: int
