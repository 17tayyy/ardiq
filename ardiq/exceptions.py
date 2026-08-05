"""Exceptions a task raises to steer its own execution."""

from __future__ import annotations


class Retry(Exception):
    """Raise inside a task to run it again, optionally after `delay_ms`.

    It still respects the task's `max_retries`; when that budget runs out the
    task fails with this exception as its error. Unlike an ordinary failure, a
    retry you asked for does not fire the `@app.on_error` hooks — only the
    final give-up does.
    """

    def __init__(
        self, message: str = "retry requested", *, delay_ms: int | None = None
    ):
        super().__init__(message)
        self.delay_ms = delay_ms
