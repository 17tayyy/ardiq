"""`executor="process"`: a sync task in a child process of its own, which a
timeout or an abort can stop, and whose logs reach the worker."""

import asyncio
import logging
import os

import process_tasks
import pytest

SUCCESS, DEAD, RETRY, ABORT = 0, 3, 2, 4


def _pack(app, fn_name, args=()):
    return app._dumps({"f": fn_name, "a": list(args), "k": {}, "t": 0})


def _process_task(app, fn, **options):
    options.setdefault("max_retries", 0)
    return app.task(executor="process", **options)(fn)


async def _run(app, fn_name, args=(), tries=1, task_id="t1"):
    outcome, env, retry_ms, _ = await app._execute(
        task_id, _pack(app, fn_name, args), tries
    )
    return outcome, app._unpack(env) if env else None, retry_ms


def _gone(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    return False


async def test_the_task_runs_in_another_process(make_app):
    app = make_app("proc-ok")
    _process_task(app, process_tasks.add)

    outcome, result, _ = await _run(app, "add", (2, 3))

    assert outcome == SUCCESS
    total, pid = result.value
    assert total == 5 and pid != os.getpid()


async def test_errors_and_retries_cross_back(make_app):
    app = make_app("proc-err")
    _process_task(app, process_tasks.boom)
    _process_task(app, process_tasks.again, max_retries=2)

    outcome, result, _ = await _run(app, "boom")
    assert outcome == DEAD and "ValueError" in result.value
    assert "broke in the child" in result.value

    outcome, _, retry_ms = await _run(app, "again")
    assert (outcome, retry_ms) == (RETRY, 10)


async def test_a_timeout_stops_the_process(make_app, tmp_path):
    app = make_app("proc-timeout")
    _process_task(app, process_tasks.hang, timeout=1)
    pid_file = tmp_path / "pid"

    outcome, result, _ = await _run(app, "hang", (str(pid_file),))

    assert outcome == DEAD and "timed out" in result.value
    assert _gone(int(pid_file.read_text()))


async def test_an_abort_stops_the_process(make_app, tmp_path, poll):
    app = make_app("proc-abort")
    _process_task(app, process_tasks.hang)
    pid_file = tmp_path / "pid"

    run = asyncio.ensure_future(_run(app, "hang", (str(pid_file),), task_id="t9"))

    async def started():
        return pid_file.exists() and bool(pid_file.read_text())

    assert await poll(started)
    app._running["t9"].cancel()
    outcome, result, _ = await asyncio.wait_for(run, timeout=10)

    assert outcome == ABORT and result.aborted
    assert _gone(int(pid_file.read_text()))


async def test_logs_from_the_child_reach_the_worker(make_app, caplog):
    app = make_app("proc-logs")
    _process_task(app, process_tasks.chatty)

    with caplog.at_level(logging.INFO):
        outcome, result, _ = await _run(app, "chatty")

    assert outcome == SUCCESS
    records = [r for r in caplog.records if r.name == "process_tasks"]
    assert [r.getMessage() for r in records] == ["hello from the child"]
    assert records[0].process == result.value


async def test_current_task_is_set_in_the_child(make_app):
    app = make_app("proc-ctx")
    _process_task(app, process_tasks.whoami)

    _, result, _ = await _run(app, "whoami", tries=2, task_id="t7")

    assert result.value == ["t7", 2]


async def test_a_process_that_dies_fails_the_task(make_app):
    app = make_app("proc-crash")
    _process_task(app, process_tasks.crash)
    _process_task(app, process_tasks.unpicklable)

    outcome, result, _ = await _run(app, "crash")
    assert outcome == DEAD and "exited with code 3" in result.value

    outcome, result, _ = await _run(app, "unpicklable")
    assert outcome == DEAD and "cannot be sent back" in result.value


def test_only_module_level_sync_functions_are_accepted(make_app):
    app = make_app("proc-refuse")

    async def asynchronous():
        pass

    def nested():
        pass

    with pytest.raises(TypeError, match="runs sync functions"):
        app.task(executor="process")(asynchronous)
    with pytest.raises(TypeError, match="module-level function"):
        app.task(executor="process")(nested)
    with pytest.raises(ValueError, match="executor must be"):
        app.task(executor="fork")(process_tasks.add)


async def test_a_worker_runs_a_process_task(redis, make_app):
    app = make_app("proc-e2e", poll_block_ms=50, burst=True)
    task = _process_task(app, process_tasks.add)

    job = await task.enqueue(20, 22)
    await asyncio.wait_for(app.run(), timeout=30)

    result = await job.result()
    assert result.success and result.value[0] == 42
