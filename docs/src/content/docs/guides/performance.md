---
title: Performance
description: How ArdiQ stacks up against five other Python task queues on throughput, memory and efficiency, with honest caveats.
head:
  - tag: title
    content: Python task queue benchmarks — ArdiQ vs arq, Taskiq, Celery
---

ArdiQ's edge isn't a single number — it's the **balance**. Because the worker loop and every
Redis round-trip run in Rust, off the GIL, ArdiQ delivers **near-top throughput at the
lowest memory of any fast queue**. No queue in the suite beats it on throughput *and*
memory at the same time.

The numbers below come from an apples-to-apples suite that runs six Redis-backed Python
queues through the same scenarios on the same machine. It's open and reproducible:
[**python-task-queue-benchmarks**](https://github.com/17tayyy/python-task-queue-benchmarks).

## Test setup

- **1,000 tasks**, **1 worker process**, **10 concurrent** tasks, **6 interleaved rounds** —
  metrics reported as `mean ± std`.
- Rounds are interleaved and the starting library rotates, so no library sits at the same
  position twice and machine drift is spread across all six rather than landing on whoever
  runs last.
- Two scenarios:
  - **`io_task`** — a 100 ms sleep (`asyncio.sleep` for async libs, `time.sleep` for sync).
  - **`cpu_task`** — 1,000 SHA-256 hashes over 1 KiB inputs per task.
- **Machine:** 8-core / 16-thread x86-64, 15 GB RAM, CPython 3.13, Redis 7.4.
- **Versions:** ArdiQ 0.5.0, arq 0.28, Taskiq 0.12.4, Streaq 6.5.0, Celery 5.5.3,
  Dramatiq 2.1.0.

## I/O-bound throughput

The `io_task` scenario is the realistic one for these libraries — async-native queues
multiplex the 10 sleeps on one event loop. With 1,000 tasks at concurrency 10 and a 100 ms
sleep, the **theoretical ceiling is 100 tasks/s**, so anything near it is essentially
network-bound.

| Queue        | Throughput (tasks/s) | Memory   |
|--------------|----------------------|----------|
| Taskiq       | 97.8                 | 91 MB    |
| **ArdiQ** 🦀 | **96.6**             | **33 MB** 🪶 |
| Dramatiq     | 94.4                 | 56 MB    |
| Streaq       | 94.1                 | 48 MB    |
| arq          | 88.5                 | 30 MB    |
| Celery       | 67.9                 | 51 MB    |

ArdiQ runs **within 1.2% of the fastest queue, practically hitting the ceiling — on a third
of that queue's memory.** It's the lightest of everything that clears 95% of the ceiling.

## CPU-bound throughput

The `cpu_task` scenario hashes under the GIL, so for *every* single-process queue the task
body is serial on one core. What this measures is really **per-task framing overhead**
(serialization, broker round-trips, bookkeeping) on top of the constant hashing cost.

| Queue        | Throughput (tasks/s) | Latency p99      | Memory   |
|--------------|----------------------|------------------|----------|
| Taskiq       | 424.9                | 2,713 ms ±1,021  | 91 MB    |
| Streaq       | 378.2                | 2,994 ms ±1,055  | 48 MB    |
| **ArdiQ** 🦀 | **375.7**            | **2,609 ms ±21** | **33 MB** 🪶 |
| arq          | 344.7                | 3,243 ms ±980    | 30 MB    |
| Celery       | 14.7                 | 73,372 ms        | 50 MB    |
| Dramatiq     | 14.7                 | 73,147 ms        | 55 MB    |

ArdiQ and Streaq are tied on throughput — 375.7 ±4.6 against 378.2 ±7.2 — but ArdiQ does it
on 31% less memory. Taskiq's 13% lead is real and reproducible; the next section explains
where it comes from.

Read the p99 column with its spread, not just its mean. ArdiQ's tail sits at **2,609 ms
±21**, while the other three async queues swing by **±1,000 ms** round to round — their
means are not far off, but you cannot predict which one you'll get. For a task queue, a
tail latency you can plan around is usually worth more than a slightly lower one you can't.

(Celery and Dramatiq sit far lower because their thread pools serialize on the GIL for this
workload — see the caveats.)

## What the Rust core costs, and what it buys

Read the CPU numbers as **milliseconds per task** instead, and the design shows through.
Every queue runs the same Python task body, so that cost is identical; the difference is
purely what each one spends dispatching:

| Queue    | ms/task | Dispatch overhead |
|----------|---------|-------------------|
| Taskiq   | 2.353   | —                 |
| Streaq   | 2.644   | +0.291 ms         |
| **ArdiQ**| 2.662   | **+0.308 ms**     |
| arq      | 2.901   | +0.548 ms         |

ArdiQ spends about **0.3 ms per task** more than Taskiq. That is the price of the boundary:
each task crosses Rust → Python to start and Python → Rust to return, taking the GIL and
bridging a future each way. Taskiq is pure Python — dispatching is an `asyncio.create_task`
and it never crosses anything.

That overhead is **fixed, not proportional**, which is the whole story:

- On a **2.4 ms** task — the benchmark's `cpu_task` — 0.3 ms is **13%**.
- On a **100 ms** task — the benchmark's `io_task` — 0.3 ms is **0.3%**, and ArdiQ lands
  within 1.2% of the lead.

So `cpu_task` is close to the worst case for this design: a thousand tiny tasks, where a
per-task toll weighs most. The work people actually build queues for — sending mail,
calling APIs, resizing images — runs in hundreds of milliseconds, where the toll disappears
and the memory stays.

Worth noting that Streaq pays +0.291 ms to ArdiQ's +0.308: the boundary isn't some exotic
Rust tax, it's within a hair of the best pure-async queue — on two thirds of its memory.

## Efficiency: throughput per MB

The metric that captures the whole trade-off is how much work a queue does per megabyte it
holds.

| Queue        | I/O (tasks/s per MB) | CPU (tasks/s per MB) |
|--------------|----------------------|----------------------|
| arq          | 2.92                 | 11.34                |
| **ArdiQ** 🦀 | **2.91**             | **11.28**            |
| Streaq       | 1.95                 | 7.82                 |
| Dramatiq     | 1.70                 | 0.27                 |
| Celery       | 1.33                 | 0.29                 |
| Taskiq       | 1.07                 | 4.66                 |

ArdiQ and arq are tied at the top — the difference is under 1%, well inside the noise. The
distinction is what each does with it: ArdiQ turns that efficiency into **9% more
throughput than arq on both workloads**, while arq spends it staying 3 MB lighter.

Against the queues that compete on speed, the gap is not close: ArdiQ does **2.7× Taskiq's
work per megabyte** on I/O.

## The takeaways

- 🪶 **Lightest of the fast queues** — 33 MB, the lowest footprint of anything at its
  performance level. (arq is 3 MB lighter but 9% slower on both workloads.)
- ⚡ **Best throughput-to-memory ratio**, tied with arq and far ahead of everything else.
- 🎯 **Predictable tail latency** — p99 of 2,609 ms ±21 on CPU work, where the other async
  queues swing ±1,000 ms between rounds.
- 📈 **Near the theoretical ceiling** on I/O work — within 1.2% of the lead, on a third of
  its memory.
- 🧱 **Rock-steady** — ArdiQ measured within ±1% across five separate runs while other
  queues moved 5–8% with the machine.

What ArdiQ is *not*: the fastest queue in raw throughput. Taskiq is 13% quicker on
CPU-bound micro-tasks, and it spends 91 MB doing it. If your tasks are tiny and memory is
free, that's the better trade. If they're normal-sized and you run more than one worker,
ArdiQ's is.

## Honest caveats

:::caution[Throughput depends on hardware and workload]
These numbers are shaped by the machine, the Redis instance, and the specific workload.
The GIL caps in-process CPU work for *every* Python queue — ArdiQ included — so CPU-bound
tasks are serial per worker; scale them out with more worker processes.
:::

- **Never compare across runs.** Only the libraries *within* one run are comparable. On this
  machine the same unchanged code measured 5–8% apart on different evenings, which is
  larger than most of the gaps above.
- **CPU parallelism isn't measured here.** All libraries run one worker; to scale CPU work
  you'd run multiple worker processes (Celery's prefork, Dramatiq's `--processes N`, or
  several async workers). This suite measures per-task overhead, not multi-core scaling.
- **Each queue uses its idiomatic dispatch path** and the same Redis instance, one at a
  time. Latency, raw per-iteration samples, and the full methodology — including how
  tail-latency is measured — live in the
  [benchmark repo](https://github.com/17tayyy/python-task-queue-benchmarks).
