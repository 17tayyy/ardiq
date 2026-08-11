---
title: Performance
description: How ArdiQ stacks up against five other Python task queues on throughput, latency, memory and efficiency, with the caveats that matter.
head:
  - tag: title
    content: Python task queue benchmarks — ArdiQ vs Taskiq, Streaq, arq, Celery
---

Six Redis-backed Python queues, one machine, one sitting, three workloads. This page is the
long version: what was run, what came out, and where the numbers flatter us and shouldn't.

The suite is open and reproducible, including the row where we lose:
[**python-task-queue-benchmarks**](https://github.com/17tayyy/python-task-queue-benchmarks).

## Test setup

- **1 worker process**, **10 concurrent** tasks, **3 interleaved rounds** — reported as
  `mean ± std`.
- Rounds are interleaved and the starting library rotates, so no library sits at the same
  position twice and machine drift is spread across all six rather than landing on whoever
  runs last.
- Three scenarios:
  - **`io_task`** — 1,000 tasks, each a 100 ms sleep (`asyncio.sleep` for async libs,
    `time.sleep` for sync).
  - **`cpu_task`** — 1,000 tasks, each 1,000 SHA-256 hashes over 1 KiB inputs.
  - **`noop_task`** — 20,000 tasks that do nothing, so what is measured is the queue's own
    cost to move a task and nothing else.
- Enqueuing is timed separately from processing, so a fast producer is never credited to the
  worker's throughput.
- **Machine:** 8-core / 16-thread x86-64, 15 GB RAM, CPython 3.13, Redis 7.4.
- **Versions:** arq 0.28, Taskiq 0.12.4, Streaq 6.5.0, Celery 5.5.3, Dramatiq 2.1.0, and
  ArdiQ from `development` — the producer fixes behind these numbers ship in the next
  release; 0.5.0 from PyPI measures about 12% lower on CPU work.

## Moving tasks: the `noop_task` scenario

A task that does nothing takes the work out of the measurement and leaves the queue. This is
the scenario the Rust core is built for.

| Queue        | Throughput (tasks/s) | Latency p99        | Memory   |
|--------------|----------------------|--------------------|----------|
| **ArdiQ** 🦀 | **2,895 ±49**        | **6,820 ms ±127**  | **34 MB** 🪶 |
| Taskiq       | 2,051 ±99            | 9,292 ms           | 91 MB    |
| Dramatiq     | 1,787 ±73            | 10,492 ms          | 56 MB    |
| Streaq       | 1,179 ±56            | 15,626 ms          | 52 MB    |
| Celery       | 861 ±39              | 21,885 ms          | 50 MB    |
| arq          | 789 ±52              | 23,500 ms          | 30 MB    |

**41% ahead of the next queue, and the tightest tail in the field.** Read those two together:
the p99 is not just lower, it is lower by 26% while ArdiQ's round-to-round spread stays at
±127 ms.

As milliseconds of overhead per task, which is what this really measures:

| Queue    | Overhead per task |
|----------|-------------------|
| **ArdiQ**| **0.345 ms**      |
| Taskiq   | 0.488 ms          |
| Dramatiq | 0.560 ms          |
| Streaq   | 0.848 ms          |
| Celery   | 1.161 ms          |
| arq      | 1.268 ms          |

ArdiQ crosses the Rust/Python boundary twice per task — once to start it, once to return its
result — and still spends less per task than the pure-Python queues that never cross
anything. That is the loop and the Redis I/O being off the GIL.

## CPU-bound throughput

The `cpu_task` body hashes under the GIL, so for *every* single-process queue the task body
is serial on one core. What varies is the framing around it.

| Queue        | Throughput (tasks/s) | Latency p99         | Memory   |
|--------------|----------------------|---------------------|----------|
| **ArdiQ** 🦀 | **394.3 ±4.7**       | 2,480 ms ±51        | **33 MB** 🪶 |
| Taskiq       | 356.4 ±18.5          | 2,185 ms ±1,032     | 91 MB    |
| Streaq       | 322.3 ±3.4           | 3,032 ms ±20        | 48 MB    |
| arq          | 282.9 ±6.1           | 3,466 ms ±75        | 30 MB    |
| Celery       | 12.5                 | 74,909 ms           | 51 MB    |
| Dramatiq     | 12.5                 | 75,781 ms           | 55 MB    |

**11% faster than Taskiq, on a third of its memory.** The p99 column is a tie worth being
honest about: Taskiq's mean tail is lower here, but it moves by ±1,032 ms between rounds
against ArdiQ's ±51, and the two swap places from one run to the next. Call it even.

## I/O-bound throughput

With 1,000 tasks at concurrency 10 and a 100 ms sleep, the **arithmetic ceiling is 100
tasks/s**. Everything near it is waiting on the network, not on the queue.

| Queue        | Throughput (tasks/s) | % of ceiling | Memory   |
|--------------|----------------------|--------------|----------|
| Taskiq       | 96.9                 | 97%          | 91 MB    |
| **ArdiQ** 🦀 | **95.3**             | **95%**      | **34 MB** 🪶 |
| Dramatiq     | 94.1                 | 94%          | 56 MB    |
| Streaq       | 91.8                 | 92%          | 48 MB    |
| arq          | 87.6                 | 88%          | 30 MB    |
| Celery       | 71.2                 | 71%          | 51 MB    |

Second, by 1.7%, in the one scenario where there is almost nothing left to win. Both queues
are up against the sleep, not against each other — the interesting column here is the third
one.

## Enqueuing

How long the *producer* is blocked staging work — the number that matters when the thing
enqueuing is a web request.

| Queue        | Tasks staged per second | 20,000 tasks take |
|--------------|-------------------------|-------------------|
| **ArdiQ** 🦀 | **81,636**              | **0.2 s**         |
| Streaq       | 27,433                  | 0.7 s             |
| Dramatiq     | 2,711                   | 7.4 s             |
| Taskiq       | 2,503                   | 8.0 s             |
| Celery       | 1,886                   | 10.6 s            |
| arq          | 999                     | 20.0 s            |

ArdiQ and Streaq stage batches through a bulk API (`enqueue_many`); the other four await one
round trip per task, which is their only option — except Celery, which has
`group(...).apply_async()` that this suite does not use. **The clean comparison is ArdiQ
against Streaq: 3× faster, both batching.** Against the rest, part of the gap is our API and
part is how the suite calls theirs.

## Efficiency: throughput per MB

| Queue        | Dispatch (tasks/s per MB) | CPU (tasks/s per MB) | I/O (tasks/s per MB) |
|--------------|---------------------------|----------------------|----------------------|
| **ArdiQ** 🦀 | **87.2**                  | **11.9**             | 2.84                 |
| Dramatiq     | 31.8                      | 0.23                 | 1.69                 |
| arq          | 26.0                      | 9.3                   | **2.88**             |
| Streaq       | 22.7                      | 6.6                   | 1.90                 |
| Taskiq       | 22.5                      | 3.9                   | 1.06                 |
| Celery       | 17.3                      | 0.25                 | 1.39                 |

On dispatch, ArdiQ does **2.7× the work per megabyte** of the next queue, and **3.9× Taskiq's**.
arq edges it on I/O by under 2% — the two are tied there, and arq is 33% slower on CPU work
and 3.7× slower on dispatch.

## The takeaways

- ⚡ **41% more tasks a second than the next queue on dispatch**, 11% more on CPU work.
- 🪶 **33 MB per worker** — a third of Taskiq's, two thirds of Streaq's. Only arq is lighter,
  by 3 MB, and it is last or second-to-last on all three.
- 🎯 **The tightest tail in the field on dispatch** — p99 6,820 ms ±127 against 9,292.
- 📈 **95% of the arithmetic ceiling** on I/O work, 1.7% off the lead.
- 🚀 **0.2 s to stage 20,000 tasks**, against 8 s for the next queue with a comparable API.

## Honest caveats

:::caution[Celery and Dramatiq's CPU numbers are an artifact of this machine]
Both land at 12.5 tasks/s on `cpu_task`, roughly a thirtieth of the async queues. That is
prefork workers thrashing the GIL, and it gets *worse* the more cores you give them — the
same code measures near 55 tasks/s on a laptop. Do not read that row as a 30× win; read it
as "this box is hostile to that model".
:::

- **The GIL caps in-process CPU work for every Python queue, ArdiQ included.** Your task body
  is serial per worker. Scale CPU-bound work out with more worker processes.
- **Never compare across runs or machines.** Only the libraries *within* one table are
  comparable. On this machine the same unchanged code has measured 5–8% apart on different
  evenings, which is larger than several of the gaps above.
- **The enqueue table is not apples-to-apples for four of the six.** See the note under it.
- **CPU parallelism isn't measured here.** All libraries run one worker; this suite measures
  per-task overhead, not multi-core scaling.
- **These numbers are ArdiQ's `development` build.** The fixes behind them are committed and
  tested but not yet released; PyPI's 0.5.0 measures about 12% lower on CPU work and roughly
  half on dispatch.

Raw per-iteration samples, the full methodology, and every scenario we run live in the
[benchmark repo](https://github.com/17tayyy/python-task-queue-benchmarks).
