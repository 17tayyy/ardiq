---
title: How ArdiQ works
description: The path a task takes from enqueue to XACK, ArdiQ's at-least-once delivery guarantee, and why the hot path is Rust, Redis Streams and Lua.
head:
  - tag: title
    content: "How ArdiQ works: Redis Streams, Lua and a Rust worker loop"
---

Three parties are involved in every task: the **producer** that enqueues it, **Redis**, and
a **worker** that runs it. All durable state lives in Redis. A worker holds nothing that
matters if it dies, which is what makes the recovery story below possible.

This page is the mechanism. If you only want to write tasks, [Defining
tasks](/guides/tasks/) is the page you want.

## The path of a task

<figure class="diagram">
  <img class="dark:sl-hidden" src="/task-lifecycle.svg" width="721" height="610" alt="A task travels from add.enqueue in your app, through publish_task.lua and a Redis stream, into the Rust core and your Python coroutine, and ends in one atomic XACK + XDEL pipeline that either completes, retries or aborts it." />
  <img class="light:sl-hidden" src="/task-lifecycle-dark.svg" width="721" height="610" alt="A task travels from add.enqueue in your app, through publish_task.lua and a Redis stream, into the Rust core and your Python coroutine, and ends in one atomic XACK + XDEL pipeline that either completes, retries or aborts it." />
  <figcaption>Every terminal state begins with the same acknowledgement, and it happens after your code has run, never before.</figcaption>
</figure>

Stage by stage:

1. **Enqueue.** Arguments are serialized (msgpack by default) and handed to the Rust core,
   which runs one Lua script: the payload is stored under `task:data:<id>` with `SET ... NX`
   and, in the same script, the task id is either appended to the priority's stream or added
   to that priority's delayed sorted set if it has a fire time. One round trip, all or
   nothing. Bulk enqueues run a sibling script that does the same per task, up to 1,000
   tasks per invocation.

2. **Pickup.** Each worker runs a producer task that polls the streams in priority order,
   highest first. Every poll issues `XAUTOCLAIM` before `XREADGROUP`, so work stranded by a
   dead worker is preferred over fresh work. When nothing is waiting, it falls back to a
   blocking `XREADGROUP` across all lanes rather than spinning.

3. **Dispatch.** Messages cross a bounded channel to the consumer tasks, one per concurrency
   slot. The consumer increments the attempt counter, adds the task to the running set, and
   reads the payload and the abort marker in a single pipelined round trip.

4. **Execution.** The core calls into Python once per task, through a single async callback,
   and awaits the resulting future. `async def` tasks run on the event loop; blocking `def`
   tasks are pushed to a thread with `asyncio.to_thread` so they never freeze the worker.

5. **Finish.** The outcome comes back as one of three codes, and each one is a single atomic
   Redis pipeline: **complete** acknowledges the entry and writes the result, **retry**
   acknowledges it and puts the task back in the delayed set with its backoff, **abort**
   completes it with an aborted result. Every one of them starts with `XACK`.

## Delivery semantics: at-least-once

**ArdiQ delivers each task at least once.** A task can run more than once. It will not be
silently dropped.

The guarantee comes from where the acknowledgement sits. `XACK` appears in exactly three
places in the core, and all three run *after* the task body has terminated, in the same
atomic pipeline that writes the result or reschedules the retry. No code path acknowledges
an entry before running it.

Until that `XACK`, the entry stays in the consumer group's pending list with the worker's
name on it. If the worker dies, the entry is still there, and after
[`idle_timeout_ms`](/reference/configuration/#idle_timeout_ms) another worker reclaims it
with `XAUTOCLAIM` and runs it again. Losing a worker costs you a repeat, never a task.

So a task runs twice when:

- a worker dies mid-task: `SIGKILL`, an OOM kill, or the machine going away,
- a worker gets wedged badly enough that its heartbeat stops while the task keeps running.

A retry is not a duplicate in this sense. It is a deliberate redelivery of the same task id,
and `tries` counts up across all of them.

:::caution[Write tasks that survive running twice]
Make the effect idempotent, or cheap to repeat. Charging a card, sending an email or
incrementing a counter are the cases to think about; `PUT`-style writes and anything keyed on
the task id are already fine. Setting `task_id` yourself also gives you an idempotent
*enqueue*, which is a different guarantee and is covered in
[Custom ids & deduplication](/guides/enqueuing/#custom-ids--deduplication).
:::

ArdiQ does not offer exactly-once, and neither does any other queue that crosses a process
boundary. The honest version of that promise is always at-least-once delivery plus effects
that tolerate a repeat.

One consequence worth knowing: the attempt counter is incremented at **pickup**, not at
failure, so a reclaimed task does not get a fresh retry budget. A task that fails after a
reclaim resumes counting where the dead worker left off. The exception is a task that takes
the whole process down every time it runs, before any Python code can decide anything: that
one gets reclaimed indefinitely, because nothing ever reports an outcome. If you have a task
that can hard-kill a worker, cap it with a
[`timeout`](/guides/tasks/#timeouts) or keep it out of the queue.

## Why the writes go through Lua

Four operations run as Lua scripts, and each one is a decision plus several writes that must
not interleave with another client.

**Staging a task.** The payload and the queue entry have to land together. Store the payload
without queueing and the task never runs; queue without the payload and a worker picks up an
entry whose data is not there. The `SET ... NX` also *decides* whether this id is new, and
that decision has to be fused to the `XADD`, or two concurrent enqueues of a
[`unique=True`](/guides/enqueuing/#unique-tasks) task both stage.

**Promoting delayed tasks.** Moving due tasks into the stream is a `ZRANGE ... BYSCORE`, then
a `ZREMRANGEBYSCORE`, then an `XADD` per task. Two workers doing that concurrently without
atomicity both read the same due set and enqueue every task twice.

**Aborting.** An abort resolves differently depending on where the task is at that instant:
running, waiting in a delayed set, or sitting in a live stream. The state can change between
the check and the write, so the whole thing is one script.

That last case is also why an abort leaves a marker rather than deleting the entry: a stream
entry can only be removed by the consumer that reads it. A queued task keeps its place, and
the worker honors the marker at pickup, reading it in the same round trip as the payload.

## Crash recovery

Workers sharing a `queue_name` join one Redis consumer group per priority stream, each under
its own consumer name. Entries that have been read but not acknowledged sit in that group's
pending list.

A dedicated heartbeat task runs at 90% of `idle_timeout_ms` and does two things: it refreshes
the worker's health key, and it issues `XCLAIM ... JUSTID` over every message the worker is
currently holding, which resets those entries' idle timers.

That second half is what makes long tasks safe. Without it, any task running longer than
`idle_timeout_ms` would be stolen and run a second time while the first was still going. With
it, "idle" means *the worker holding this stopped reporting*, not *this is taking a while*.

## Backpressure

A worker starts with `prefetch` permits. The producer task only ever asks Redis for as many
entries as it has permits, and when it runs out it parks instead of reading. Each consumer
returns a permit when its task finishes and wakes the producer.

The channel between them is bounded at `prefetch` as well, so the two bounds agree. The
practical effect is that a worker never holds more than `prefetch` unacknowledged entries.
That caps its memory, and it caps how much work is stranded in the pending list waiting on a
reclaim if the worker dies.

## Where Rust ends and Python begins

The two sides meet at exactly one function: a callback taking `(task_id, payload, tries,
aborted)` and returning `(outcome, result_bytes, retry_ms)`.

The core attaches to the interpreter to build the coroutine, converts it to a Rust future,
awaits that future, then attaches once more to read the returned tuple. Everything between
those two attaches, which is your task actually running plus every Redis round trip around
it, happens without the core holding the GIL. The loop, the polling, the delayed-task
promotion and the reclaim logic never touch the interpreter at all.

Two GIL acquisitions per task, each measured in microseconds, is the whole per-task cost of
the boundary. That is where the [dispatch numbers](/guides/performance/#moving-tasks-the-noop_task-scenario)
come from: ArdiQ crosses a language boundary twice per task and still spends less time per
task than the pure-Python queues that never cross anything.

## The Redis keyspace

Every key is namespaced by `queue_name`, so unrelated workloads can share one Redis. With
`Ardiq(queue_name="emails")` the prefix is `ardiq:emails`.

| Key | Type | Holds |
|---|---|---|
| `:queues:<priority>` | stream | live entries for one lane |
| `:queues:delayed:<priority>` | zset | delayed, scheduled and retrying tasks, scored by fire time |
| `:task:data:<id>` | string | the serialized call, deleted when the task finishes |
| `:task:results:<id>` | string | the result envelope, TTL from `result_ttl_ms` |
| `:task:retry:<id>` | string | attempt counter, incremented at each pickup |
| `:task:abort:<id>` | string | abort marker, honored at pickup |
| `:index:running` | set | task ids executing right now |
| `:index:results` | zset | result ids by expiry, swept by the heartbeat |
| `:health:<worker_id>` | string | worker heartbeat, expires after `idle_timeout_ms` |
| `:result:channel:<id>` | pub/sub | published when a result lands, so `await job.result()` does not poll |
| `:abort:channel` | pub/sub | published on abort, so a running task can be cancelled |

The consumer group on every stream is named `workers`.

## Next steps

- [Handling failures](/guides/errors/) for retries, backoff and error hooks.
- [Running a worker](/guides/worker/) for concurrency, prefetch and shutdown.
- [Performance](/guides/performance/) for what all of this buys, measured against five other
  queues.
