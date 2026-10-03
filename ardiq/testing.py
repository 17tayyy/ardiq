"""Run tasks without Redis or a worker, for tests.

```python
from ardiq.testing import inline

async def test_add():
    async with inline(app):
        job = await add.enqueue(2, 3)
        assert (await job.result()).value == 5
```
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Iterable
from typing import TYPE_CHECKING, Any, cast

from ardiq._core import ArdiqCore
from ardiq.app import DEAD, RETRY
from ardiq.app import _now_ms as now_ms

if TYPE_CHECKING:
    from ardiq.app import Ardiq


class _InlineCore:
    """Stands in for the Rust core: runs each task the moment it is enqueued
    and keeps results in memory. Config reads go to the real core."""

    def __init__(self, app: Ardiq, core: ArdiqCore):
        self._app = app
        self._core = core
        self._results: dict[str, bytes] = {}
        self._running: dict[str, tuple[bytes, int]] = {}  # id -> (payload, tries)
        self._dead: dict[str, tuple[str, bytes, bytes, str, int]] = {}

    def __getattr__(self, name: str) -> Any:
        return getattr(self._core, name)

    async def enqueue(
        self,
        task_id: str,
        payload: bytes,
        priority: str | None,
        delay_ms: int,
        schedule_ms: int,
        expire_ms: int,
        unique: bool,
    ) -> None:
        # Same id already running: Redis would refuse the second one too.
        if task_id in self._running:
            return
        self._results.pop(task_id, None)
        lane = priority or self._app.default_priority
        await asyncio.ensure_future(self._run(task_id, payload, lane))

    async def enqueue_many(self, items: Iterable[tuple]) -> None:
        for item in items:
            await self.enqueue(*item)

    async def _run(self, task_id: str, payload: bytes, priority: str) -> None:
        # Its own asyncio task, so an abort cancels this run and not the caller.
        tries = 1
        try:
            while True:
                self._running[task_id] = (payload, tries)
                outcome, env, _ = await self._app._execute(task_id, payload, tries)
                if outcome != RETRY:
                    self._results[task_id] = env
                    if outcome == DEAD:
                        dead = (task_id, payload, env, priority, now_ms())
                        self._dead[task_id] = dead
                    return
                tries += 1  # no backoff: a test should not sleep
        finally:
            self._running.pop(task_id, None)

    async def run(self, callback: Any) -> None:
        raise RuntimeError("inline mode runs tasks on enqueue; there is no worker")

    def stop(self) -> None:
        pass

    async def queue_size(self) -> int:
        return 0

    async def result(self, task_id: str) -> bytes | None:
        return self._results.get(task_id)

    async def await_result(self, task_id: str, timeout_ms: int) -> bytes | None:
        # Every task has finished by the time enqueue returns, so waiting
        # longer could never change the answer.
        return self._results.get(task_id)

    async def abort(self, task_id: str, result: bytes) -> bool:
        running = self._app._running.get(task_id)
        if running is None or running.done():
            return False
        running.cancel()
        return True

    async def status(self, task_id: str) -> str:
        if task_id in self._results:
            return "complete"
        if task_id in self._running:
            return "running"
        return "not_found"

    async def dead_count(self) -> int:
        return len(self._dead)

    async def dead_list(self, limit: int) -> list[tuple[str, bytes, bytes, str, int]]:
        return list(reversed(self._dead.values()))[: max(limit, 0)]

    async def dead_get(self, task_id: str) -> tuple[str, bytes, bytes, str, int] | None:
        return self._dead.get(task_id)

    async def dead_replay(self, task_id: str, payload: bytes, priority: str) -> bool:
        if self._dead.pop(task_id, None) is None:
            return False
        await self.enqueue(task_id, payload, priority, 0, 0, 0, True)
        return True

    async def dead_delete(self, task_id: str) -> bool:
        return self._dead.pop(task_id, None) is not None

    async def task_info(self, task_id: str) -> tuple[bytes | None, int, int]:
        payload, tries = self._running.get(task_id, (None, 0))
        return payload, tries, 0


@contextlib.asynccontextmanager
async def inline(app: Ardiq) -> AsyncIterator[None]:
    """Run `app`'s tasks inline while the block is open: `enqueue` executes the
    task before returning, so its result is already there. No Redis needed.

    Retries, timeouts, `on_error` hooks, `current_task()` and the serializer
    behave as on a worker, and the `@app.lifespan` hook runs around the block.
    Retries run back to back, and delays and schedules are ignored.
    """
    real = app._core
    app._core = cast(ArdiqCore, _InlineCore(app, real))
    try:
        async with app._lifespan_scope():
            yield
    finally:
        app._core = real
