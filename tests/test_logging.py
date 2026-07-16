"""Task lifecycle logging in `Ardiq._execute`: levels, fields, and that user
logs emitted from inside a task are not intercepted or suppressed."""

import logging


def _pack(app, fn_name, args=(), kwargs=None):
    return app._dumps({"f": fn_name, "a": list(args), "k": kwargs or {}, "t": 0})


async def test_task_started_and_succeeded_are_debug(make_app, caplog):
    app = make_app("log_ok")

    @app.task()
    async def add(a, b):
        return a + b

    with caplog.at_level(logging.DEBUG, logger="ardiq"):
        outcome, _, _ = await app._execute("t1", _pack(app, "add", (1, 2)), 1)

    assert outcome == 0  # SUCCESS
    records = {r.message: r for r in caplog.records if r.name == "ardiq"}
    started = next(m for m in records if m.startswith("task started"))
    succeeded = next(m for m in records if m.startswith("task succeeded"))
    assert records[started].levelno == logging.DEBUG
    assert records[succeeded].levelno == logging.DEBUG
    assert "id=t1" in started and f"worker={app.worker_id}" in started
    assert "duration_ms=" in succeeded


async def test_task_retry_scheduled_is_warning(make_app, caplog):
    app = make_app("log_retry")

    @app.task(max_retries=2)
    def boom():
        raise ValueError("boom")

    with caplog.at_level(logging.DEBUG, logger="ardiq"):
        outcome, _, _ = await app._execute("t2", _pack(app, "boom"), 1)

    assert outcome == 2  # RETRY
    record = next(r for r in caplog.records if r.message.startswith("task retry"))
    assert record.levelno == logging.WARNING
    assert "delay_ms=" in record.message and "error=" in record.message


async def test_task_failed_terminal_is_error(make_app, caplog):
    app = make_app("log_fail")

    @app.task(max_retries=0)
    def boom():
        raise ValueError("boom")

    with caplog.at_level(logging.DEBUG, logger="ardiq"):
        outcome, _, _ = await app._execute("t3", _pack(app, "boom"), 1)

    assert outcome == 1  # FAILURE
    record = next(r for r in caplog.records if r.message.startswith("task failed"))
    assert record.levelno == logging.ERROR
    assert "duration_ms=" in record.message and "error=" in record.message


async def test_unknown_task_is_error(make_app, caplog):
    app = make_app("log_unknown")

    with caplog.at_level(logging.DEBUG, logger="ardiq"):
        outcome, _, _ = await app._execute("t4", _pack(app, "does_not_exist"), 1)

    assert outcome == 1  # FAILURE
    record = next(r for r in caplog.records if r.message.startswith("task unknown"))
    assert record.levelno == logging.ERROR
    assert "does_not_exist" in record.message


async def test_user_logs_inside_async_task_are_visible(make_app, caplog):
    app = make_app("log_user_async")
    task_logger = logging.getLogger("tests.user_task")

    @app.task()
    async def greet():
        task_logger.info("hello from async task")

    with caplog.at_level(logging.INFO, logger="tests.user_task"):
        await app._execute("t5", _pack(app, "greet"), 1)

    assert any(r.message == "hello from async task" for r in caplog.records)


async def test_user_logs_inside_sync_task_are_visible(make_app, caplog):
    app = make_app("log_user_sync")
    task_logger = logging.getLogger("tests.user_task")

    @app.task()
    def greet():
        task_logger.info("hello from sync task")

    with caplog.at_level(logging.INFO, logger="tests.user_task"):
        await app._execute("t6", _pack(app, "greet"), 1)

    assert any(r.message == "hello from sync task" for r in caplog.records)
