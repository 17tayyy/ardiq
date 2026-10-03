---
title: Dead letter queue
description: Tasks that fail for good are kept with their arguments, so you can inspect them and replay them once the cause is fixed.
---

A task that fails for good is kept in the dead letter queue, together with its arguments
and its error. It stays there, with no TTL, until you replay it or delete it. Its
`TaskResult` still expires with `result_ttl_ms` as usual. The dead letter entry is a
separate copy.

## What lands there

| Outcome | In the dead letter queue? |
|---|---|
| The task raised and has no retries left | Yes |
| The last attempt hit its `timeout` | Yes |
| `raise Retry(...)` with no retries left | Yes |
| The worker does not know the task (a deploy that is not out yet) | Yes |
| The task succeeded, or failed and will retry | No |
| The task was [aborted](/guides/aborting/) | No |

The unknown-task case is worth noting. If a producer sends a task before the workers
that know it are deployed, those tasks are not lost: replay them once the deploy is out.

## From the command line

```console
$ ardiq dlq list myapp:app
3f2a9c...  charge  2026-10-03 09:12:44Z  tries=4  ValueError('card declined')
1 of 1 dead tasks

$ ardiq dlq replay myapp:app 3f2a9c...
replayed 1 of 1

$ ardiq dlq replay myapp:app --all
$ ardiq dlq delete myapp:app 3f2a9c...
```

See the [CLI reference](/reference/cli/#ardiq-dlq) for every option.

## From Python

```python
for dead in await app.dead_letters(limit=20):    # newest first
    print(dead.task_id, dead.fn_name, dead.args, dead.kwargs, dead.error)

print(await app.dead_count())

job = await app.replay(dead.task_id)              # None if it is not there
await app.delete_dead(other_id)                   # False if it is not there
```

Each entry is a [`DeadLetter`](/reference/api/#deadletter): the task id, name, arguments,
lane, error, tries, and when it was enqueued and when it failed.

## How a replay works

A replay puts the task back in its lane with the **same id** and the same arguments. Its
retry budget starts over, and its old failed result is cleared, so a `Job` you already
hold reports `queued` and then the new outcome rather than the old failure.

Moving the task from the dead letter queue back into its lane is a single atomic step in
Redis. Two replays of the same task cannot both enqueue it, and if the replay fails
again, it lands back in the dead letter queue as a fresh entry.

If the lane the task ran in is no longer one of the app's `priorities`, `replay` raises
`ValueError` instead of sending it where no worker reads, and the entry stays.

## Keeping it small

Entries never expire on their own, which is the point: a failure nobody looked at is
still there when someone does. To keep the queue from growing without bound, look at
`dead_count()` from your monitoring, and delete what you will not replay:

```python
for dead in await app.dead_letters(limit=1000):
    if dead.fn_name == "send_newsletter":
        await app.delete_dead(dead.task_id)
```

In tests, [`ardiq.testing.inline`](/guides/testing/) keeps a dead letter queue in memory
with the same API.
