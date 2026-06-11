---
title: Introduction
description: What ArdiQ is, how the Rust core and Python API fit together, and when to reach for it.
---

ArdiQ is a distributed task queue: you enqueue function calls from anywhere in your
application, and one or more worker processes pick them up and run them in the background —
backed by [Redis streams](https://redis.io/docs/latest/develop/data-types/streams/).

What makes ArdiQ different is the split:

- The **worker loop and all Redis I/O run in Rust**, on [tokio](https://tokio.rs), through
  [PyO3](https://pyo3.rs). This part never touches the GIL while it's polling streams,
  scheduling delayed jobs, or reclaiming work from crashed workers.
- **You write tasks in plain Python.** The Rust core calls back into a single async
  function for each task; the GIL is held only for the microseconds it takes to start the
  task and read its result.

The result is top-tier throughput at a fraction of the memory of comparable Python queues,
because the hot path — the loop and the network — stays off the interpreter.

## Feature overview

- 🦀 **Rust core** — the loop and Redis I/O run on tokio, off the GIL.
- **Priority queues** — higher-priority tasks are consumed first.
- **Delayed & scheduled** tasks (`delay_ms` / `schedule_ms`).
- **Automatic retries** with backoff, configurable per task.
- **Crash recovery** — in-flight tasks of a dead worker are reclaimed (`XAUTOCLAIM`).
- **Results** with TTL, plus task **status** (`queued` / `scheduled` / `running` /
  `complete` / `not_found`).
- **Sync & async tasks** — blocking sync functions run in a thread pool.
- **CLI worker** (`ardiq run module:app`) and **burst mode** (drain the queue and exit).

## When to use ArdiQ

**Reach for ArdiQ when you want:**

- **High concurrency on a small footprint** — async-native, with the loop and Redis I/O in
  Rust, so one process does a lot without eating memory.
- **A modern, typed API** — `@app.task`, awaitable enqueue, `Job` handles, results and
  status built in.
- **Reliability out of the box** — priorities, retries with backoff, delayed and scheduled
  tasks, and crash recovery via Redis consumer groups.
- **Redis you already run** — no extra broker to operate.

**Consider the alternatives when:**

- **You need to saturate many CPU cores in one process.** Like *every* single-process
  Python queue, ArdiQ runs your task body under the GIL, so CPU-bound work is serial per
  worker (scale out with more workers). For heavy CPU fan-out, a prefork model (Celery,
  Dramatiq) can be simpler.
- **You need a large, battle-tested ecosystem today.** Celery has years of integrations,
  schedulers, and dashboards. ArdiQ is young and moving fast.
- **You can't run Redis.** ArdiQ is Redis-only by design.

ArdiQ sits alongside **arq / Taskiq / Streaq** as a modern async queue — its edge is the
Rust core (memory and per-task overhead) and a batteries-included API.

## Next steps

Head to [Getting started](/guides/getting-started/) to install ArdiQ and run your first
worker.
