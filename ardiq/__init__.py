"""ArdiQ: a fast distributed task queue with a Rust core and a Python API.

The Rust core (`ardiq._core`) owns the worker loop and all Redis I/O; this
package is the Python surface — the `Ardiq` app, the `@task` / `@cron`
decorators, and the task handles.
"""

from __future__ import annotations

from ardiq._core import ArdiqCore as ArdiqCore  # re-exported for tests/tooling
from ardiq.app import Ardiq
from ardiq.context import current_task
from ardiq.exceptions import ArdiqError, BrokerError, Retry
from ardiq.models import (
    DeadLetter,
    EnqueueContext,
    ErrorContext,
    ExecutionContext,
    State,
    TaskContext,
    TaskInfo,
    TaskResult,
)
from ardiq.tasks import Job, PreparedTask, Task

__all__ = [
    "Ardiq",
    "ArdiqError",
    "BrokerError",
    "DeadLetter",
    "EnqueueContext",
    "ErrorContext",
    "ExecutionContext",
    "Job",
    "PreparedTask",
    "Retry",
    "State",
    "Task",
    "TaskContext",
    "TaskInfo",
    "TaskResult",
    "current_task",
]
