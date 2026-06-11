---
title: Results & introspection
description: Read task outcomes with Job.result, check status, and inspect in-flight tasks with info.
---

Every enqueue returns a [`Job`](/reference/api/#job) — a lightweight handle (just the app
plus a task id) you use to fetch the outcome, the current status, or metadata about a task
still in flight.

```python
job = await add.enqueue(2, 3)
```

## Fetching the result

`await job.result(timeout=...)` returns a [`TaskResult`](/reference/api/#taskresult) once
the task finishes.

```python
# poll once: returns the result now, or None if it isn't ready yet
result = await job.result()

# wait up to 5 seconds, raising TimeoutError if it never lands
result = await job.result(timeout=5)
```

- **Without `timeout`** it returns the stored result immediately, or `None` if the task
  hasn't completed.
- **With `timeout`** (seconds) it polls until the result is stored, raising `TimeoutError`
  if it isn't in time.

### The `TaskResult`

```python
TaskResult(success=True, value=5, tries=1,
           enqueue_time=..., start=..., finish=...)
```

| Field          | Description |
|----------------|-------------|
| `success`      | `True` if the task returned, `False` if it failed after all retries. |
| `value`        | The return value on success; the error's `repr` on failure. |
| `tries`        | How many attempts it took. |
| `enqueue_time` | Epoch ms when the task was enqueued. |
| `start`        | Epoch ms when execution started. |
| `finish`       | Epoch ms when execution finished. |
| `duration_ms`  | Convenience property: `finish - start`. |

```python
result = await job.result(timeout=5)
if result.success:
    print(result.value, f"in {result.duration_ms}ms after {result.tries} tries")
else:
    print("failed:", result.value)
```

:::note[Result lifetime]
Results live for `result_ttl_ms` (default 5 minutes) — see
[Configuration](/reference/configuration/). After that, `result()` returns `None`.
:::

## Checking status

`await job.status()` returns a string describing where the task is:

| Status      | Meaning |
|-------------|---------|
| `queued`    | Waiting in a priority lane, ready to run. |
| `scheduled` | Waiting in the delayed/scheduled set for its time to come. |
| `running`   | Currently being executed by a worker. |
| `complete`  | Finished; a result is available (until its TTL expires). |
| `not_found` | Unknown id — never enqueued, or aged out. |

```python
print(await job.status())   # 'queued'
```

## Inspecting in-flight tasks

`await job.info()` returns a [`TaskInfo`](/reference/api/#taskinfo) snapshot of an
**unfinished** task (queued, scheduled, or running), or `None` if it's finished or unknown.
Use `result()` for finished tasks and `info()` to introspect what's still pending.

```python
info = await job.info()
if info is not None:
    print(info.fn_name, info.args, info.kwargs)
    print("status:", info.status, "tries:", info.tries)
    if info.scheduled_at is not None:
        print("scheduled for epoch-ms", info.scheduled_at)
```

| Field          | Description |
|----------------|-------------|
| `task_id`      | The job id. |
| `fn_name`      | Registered task name. |
| `args`         | Positional arguments tuple. |
| `kwargs`       | Keyword arguments dict. |
| `enqueue_time` | Epoch ms when it was enqueued. |
| `tries`        | Attempts so far. |
| `status`       | Same values as `status()` above. |
| `scheduled_at` | Epoch ms if it's waiting in the delayed set, else `None`. |

## App-level access

If you have a task id but not the `Job`, the same calls exist on the app:

```python
await app.result(task_id, timeout=5)
await app.status(task_id)
await app.info(task_id)
await app.queue_size()      # number of jobs waiting across lanes
```
