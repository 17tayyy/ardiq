---
title: Middleware
description: Wrap every task attempt with @app.middleware, and send headers from producer to worker with @app.on_enqueue, for tracing, metrics and per-task context.
---

Middleware wraps every attempt a worker makes at a task. It is the place for anything
that should happen around all tasks without editing each one: metrics, tracing, logging
context, a database session per task.

```python
import time


@app.middleware
async def timing(ctx, call_next):
    start = time.monotonic()
    try:
        return await call_next()
    finally:
        metrics.observe(ctx.name, time.monotonic() - start)
```

A middleware is an async function of `(ctx, call_next)`. Await `call_next()` to run the
rest of the chain and the task, and return what it returns. If you are used to
Starlette or FastAPI's `@app.middleware("http")`, it is the same shape.

## What a middleware can do

- **Observe.** Read `ctx` and time the call. A `try`/`finally` runs on success, failure,
  timeout and abort alike.
- **Change the result.** Whatever it returns becomes the task's result.
- **Fail the attempt.** An exception it raises counts as the task raising: it is retried
  under `max_retries`, reaches the `@app.on_error` hooks, and ends in the
  [dead letter queue](/guides/dead-letters/) when the retries run out.
- **Set context.** A `contextvars.ContextVar` set before `call_next()` is visible inside
  the task, sync tasks included, since their thread starts with a copy of the context.

`ctx` is an `ExecutionContext`:

| Field | Type | Description |
|---|---|---|
| `task_id` | `str` | The job id. |
| `name` | `str` | The task's registered name. |
| `tries` | `int` | The attempt being made, counting from 1. |
| `args` | `tuple` | Positional arguments. |
| `kwargs` | `dict` | Keyword arguments. |
| `headers` | `dict` | What the producer's `@app.on_enqueue` hooks attached; empty if none. |

## Order

Middleware runs in the order you register it, the first one outermost:

```python
@app.middleware
async def outer(ctx, call_next): ...   # runs first, finishes last

@app.middleware
async def inner(ctx, call_next): ...   # runs closest to the task
```

The task's `timeout` covers the task alone, so a middleware sees the `TimeoutError` and
its own time is not counted against the task. An [abort](/guides/aborting/) reaches
middleware as `asyncio.CancelledError`, which a `finally` block handles.

A middleware can also be an object with an `async def __call__(self, ctx, call_next)`,
which is the usual shape for a reusable integration that takes configuration.

## Headers: from producer to worker

Some context has to travel with the task: a trace id, the request that caused it, a
tenant. `@app.on_enqueue` runs for every task as it is enqueued, through `enqueue`,
`send`, `enqueue_many` or a cron, and whatever it puts in `ctx.headers` is sent with
the task and comes back in the middleware's `ctx.headers`.

```python
@app.on_enqueue
def attach_request_id(ctx):
    ctx.headers["request_id"] = request_id_var.get()


@app.middleware
async def restore_request_id(ctx, call_next):
    token = request_id_var.set(ctx.headers.get("request_id"))
    try:
        return await call_next()
    finally:
        request_id_var.reset(token)
```

The hook gets an `EnqueueContext` with `task_id`, `name`, `args`, `kwargs` and the
`headers` dict to fill. It may be sync or async. If it raises, the enqueue raises and
nothing is sent, because a task that silently lost its trace or tenant is harder to
debug than one that was never sent.

Headers go through the serializer with the rest of the payload, so keep them to values
it can encode. They do not change a [unique](/guides/enqueuing/#unique-tasks) task's
identity, and a [replay](/guides/dead-letters/) keeps the headers of the original
enqueue.

## Example: OpenTelemetry

Tracing joins both halves. The producer injects the current trace context into the
headers, and the worker continues it, so a task shows up in the trace of the request
that enqueued it:

```python
from opentelemetry import propagate, trace

tracer = trace.get_tracer("myapp.tasks")


@app.on_enqueue
def inject_trace(ctx):
    propagate.inject(ctx.headers)


@app.middleware
async def trace_task(ctx, call_next):
    parent = propagate.extract(ctx.headers)
    with tracer.start_as_current_span(f"task {ctx.name}", context=parent) as span:
        span.set_attribute("ardiq.task_id", ctx.task_id)
        span.set_attribute("ardiq.tries", ctx.tries)
        return await call_next()
```

## Compared with on_error

`@app.on_error` is a notification. It runs after an attempt fails and cannot change
anything. Middleware sits in the call itself. Use `on_error` to report failures, for
example to Sentry, and middleware for anything that needs to run before the task or
shape its outcome.
