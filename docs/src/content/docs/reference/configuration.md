---
title: Configuration
description: Every option accepted by Ardiq(...) — Redis connection, priorities, concurrency, TTLs and more.
---

An app is configured entirely through the `Ardiq(...)` constructor:

```python
from ardiq import Ardiq

app = Ardiq(
    redis_url="redis://localhost:6379",
    queue_name="emails",
    priorities=["low", "default", "high"],
    concurrency=32,
    result_ttl_ms=600_000,
)
```

## Options

| Option | Default | Description |
|---|---|---|
| `redis_url` | `redis://localhost:6379` | Redis connection URL. |
| `queue_name` | `"default"` | Logical queue — namespaces all Redis keys for this app. |
| `priorities` | `["default"]` | Priority lane names, **lowest-first**. Higher lanes drain first. |
| `concurrency` | `16` | Max tasks running at once in a worker. |
| `prefetch` | `concurrency * 2` | Max tasks held in memory; drives backpressure against Redis. |
| `idle_timeout_ms` | `60000` | When an unrenewed in-flight task may be reclaimed by another worker. |
| `result_ttl_ms` | `300000` | How long results live. `0` drops results immediately; a negative value keeps them forever. |
| `burst` | `False` | Exit once the queue drains (also settable via `app.burst` or the CLI `--burst`). |
| `serializer` | msgpack | `Callable[[Any], bytes]` used to encode arguments and results. |
| `deserializer` | msgpack | `Callable[[bytes], Any]` used to decode them. |

:::note
`redis_url`, `queue_name`, and `priorities` are positional-or-keyword; `serializer` and
`deserializer` are keyword-only. The remaining options (`concurrency`, `prefetch`,
`idle_timeout_ms`, `result_ttl_ms`, `burst`) are forwarded to the Rust core as keyword
arguments.
:::

## Notes on specific options

### `queue_name`

Each app/worker pair operates on one logical queue. Workers sharing a `queue_name` form a
Redis consumer group and split the work between them. Use distinct names to isolate
unrelated workloads on the same Redis instance.

### `priorities`

The list is **lowest-priority first**. With `["low", "default", "high"]`, a worker drains
`high` before `default` before `low`. A task's lane comes from its
[`@task(priority=...)`](/guides/tasks/#priorities) default or a per-call
[`.options(priority=...)`](/guides/enqueuing/#per-call-options) override.

### `concurrency` and `prefetch`

`concurrency` caps how many tasks execute simultaneously. `prefetch` caps how many are
pulled into memory ahead of execution; a larger prefetch smooths bursts but holds more work
off Redis. The default `prefetch = concurrency * 2` is a sensible starting point.

### `idle_timeout_ms`

A running task periodically renews its claim via a heartbeat. If a worker dies, its
in-flight tasks stop renewing; after `idle_timeout_ms` another worker reclaims them
(`XAUTOCLAIM`). Lower it for faster recovery, raise it if tasks legitimately run long
without renewing.

### `result_ttl_ms`

Controls result retention:

- **positive** — keep results this many ms (default 5 minutes).
- **`0`** — don't store results at all.
- **negative** — keep results forever (you manage cleanup yourself).

### `serializer` / `deserializer`

See [Serialization](/guides/serialization/). Every process on a queue must use the same
codec.
