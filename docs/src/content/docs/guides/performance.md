---
title: Performance
description: How ArdiQ stacks up against five other Python task queues on throughput, memory and efficiency, with honest caveats.
---

ArdiQ's edge isn't a single number — it's the **balance**. Because the worker loop and every
Redis round-trip run in Rust, off the GIL, ArdiQ delivers **near-top throughput at the
lowest memory of any fast queue**, which gives it the best throughput-to-memory ratio in
the field.

The numbers below come from an apples-to-apples suite that runs six Redis-backed Python
queues through the same scenarios on the same machine. It's open and reproducible:
[**python-task-queue-benchmarks**](https://github.com/17tayyy/python-task-queue-benchmarks).

## Efficiency: throughput per MB

The metric that captures the whole trade-off is how much work a queue does per megabyte of
memory it holds. ArdiQ leads it.

| Queue        | I/O (tasks/s per MB) | CPU (tasks/s per MB) |
|--------------|----------------------|----------------------|
| **ArdiQ** 🦀 | **2.9**              | **11.2**             |
| arq          | 2.9                  | 10.5                 |
| Streaq       | 1.9                  | 7.2                  |
| Taskiq       | 1.1                  | 4.1                  |
| Celery       | 1.4                  | 0.3                  |
| Dramatiq     | 1.7                  | 0.2                  |

ArdiQ does the most work per megabyte of any queue tested — roughly **2.7× Taskiq's I/O
efficiency**. (arq matches it on I/O efficiency, but only by running ~10% slower; ArdiQ
stays this light *while* sitting near the throughput ceiling.)

## Test setup

- **1,000 tasks**, **1 worker process**, **10 concurrent** tasks, **3 iterations** —
  metrics reported as `mean ± std`.
- Two scenarios:
  - **`io_task`** — a 100 ms sleep (`asyncio.sleep` for async libs, `time.sleep` for sync).
  - **`cpu_task`** — 1,000 SHA-256 hashes over 1 KiB inputs per task.
- **Machine:** 8-core / 16-thread x86-64, 15 GB RAM, CPython 3.13, Redis 7.4.
- **Versions:** ArdiQ 0.1.1, arq 0.28, Taskiq 0.12.4, Streaq 6.5.0, Celery 5.5.3,
  Dramatiq 2.1.0.

## I/O-bound throughput

The `io_task` scenario is the realistic one for these libraries — async-native queues
multiplex the 10 sleeps on one event loop. With 1,000 tasks at concurrency 10 and a 100 ms
sleep, the **theoretical ceiling is 100 tasks/s**, so anything near it is essentially
network-bound.

| Queue        | Throughput (tasks/s) | Memory   |
|--------------|----------------------|----------|
| **ArdiQ** 🦀 | **98.6**             | **34 MB** 🪶 |
| Taskiq       | 97.9                 | 92 MB    |
| Dramatiq     | 93.5                 | 56 MB    |
| Streaq       | 93.4                 | 48 MB    |
| arq          | 87.7                 | 30 MB    |
| Celery       | 71.7                 | 51 MB    |

ArdiQ runs **within ~1% of the fastest queue, practically hitting the network ceiling — at
roughly a third of that queue's memory.** It's the lightest of every queue that clears 90%
of the ceiling.

## CPU-bound throughput

The `cpu_task` scenario hashes under the GIL, so for *every* single-process queue the task
body is serial on one core. What this measures is really **per-task framing overhead**
(serialization, broker round-trips, bookkeeping) on top of the constant hashing cost.

| Queue        | Throughput (tasks/s) | Memory   |
|--------------|----------------------|----------|
| **ArdiQ** 🦀 | **389.3**            | **34 MB** 🪶 |
| Taskiq       | 388.1                | 94 MB    |
| Streaq       | 353.8                | 49 MB    |
| arq          | 317.6                | 30 MB    |
| Celery       | 13.8                 | 52 MB    |
| Dramatiq     | 13.8                 | 56 MB    |

Again ArdiQ is effectively tied for the lead on throughput, at a third of the leader's
memory. (Celery and Dramatiq sit far lower here because their thread pools serialize on the
GIL for this workload — see the caveats.)

## The takeaways

- ⚡ **Best throughput-to-memory ratio** — ArdiQ does the most work per megabyte of any
  queue in the suite.
- 🪶 **Lightest of the fast queues** — ~34 MB, the lowest footprint of anything at its
  performance level. (arq is marginally lighter in absolute terms but meaningfully slower.)
- 🏆 **Among the fastest** — within ~1% of the leader on both workloads.
- 📈 **Near the theoretical ceiling** on I/O work — practically network-bound, with nothing
  lost to scheduling.
- 🎯 **Rock-steady** — negligible variance run to run (low `std`).

## Honest caveats

:::caution[Throughput depends on hardware and workload]
These numbers are shaped by the machine, the Redis instance, and the specific workload.
The GIL caps in-process CPU work for *every* Python queue — ArdiQ included — so CPU-bound
tasks are serial per worker; scale them out with more worker processes.
:::

- **CPU parallelism isn't measured here.** All libraries run one worker; to scale CPU work
  you'd run multiple worker processes (Celery's prefork, Dramatiq's `--processes N`, or
  several async workers). This suite measures per-task overhead, not multi-core scaling.
- **Each queue uses its idiomatic dispatch path** and the same Redis instance, one at a
  time. Latency, raw per-iteration samples, and the full methodology — including how
  tail-latency is measured — live in the
  [benchmark repo](https://github.com/17tayyy/python-task-queue-benchmarks).
