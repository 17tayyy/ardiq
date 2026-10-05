---
title: CLI
description: The ardiq command-line interface for running workers, managing the dead letter queue, and checking a deployment.
---

The `ardiq` command comes with the base install — it pulls in no dependencies of its own,
so a worker process carries nothing but the library:

```console
$ pip install ardiq
$ ardiq --help
```

## `ardiq run`

Run a worker for the given app.

```console
$ ardiq run MODULE:ATTR [OPTIONS]
```

### Argument

| Argument | Description |
|---|---|
| `MODULE:ATTR` | Import path to your `Ardiq` instance, e.g. `example:app` or `myproject.worker:app`. |

ArdiQ imports `MODULE` (registering every `@app.task`), looks up `ATTR`, and runs that app.
The module must be importable from your current working directory / `PYTHONPATH`.

### Options

| Option | Alias | Description |
|---|---|---|
| `--burst` | `-b` | Process everything currently queued, then exit. |
| `--verbose` | `-v` | DEBUG-level logging, including the Rust core's logs. |
| `--quiet` | `-q` | Skip the startup banner; log a one-line summary instead. |
| `--workers N` | `-w` | Run N worker processes instead of one (default: 1). |

### Examples

```console
$ ardiq run example:app                # long-running worker
$ ardiq run example:app --burst        # drain the queue and exit
$ ardiq run myproject.worker:app -v    # verbose logging
$ ardiq run example:app --workers 4    # four worker processes
```

### `--workers`

One worker is one process, and one process runs your task bodies on one core,
because the GIL sees to that. `--workers N` starts N against the same queue and
supervises them: the banner is printed once, each child logs under its own
`worker_id`, and **SIGINT**/**SIGTERM** reach all of them.

The processes are independent consumers of the same Redis streams, so Redis
hands each task to exactly one of them; nothing needs to be shared or
coordinated. Point N at your cores for CPU-bound work; for I/O-bound work
[`concurrency`](/reference/configuration/) inside one process is usually the
cheaper knob.

If a worker exits non-zero, the supervisor stops the others and exits with that
code: a crashed worker fails the whole deployment rather than quietly running
short-handed. Under `--burst` every worker exits when the queue is drained and
the supervisor exits `0`.

:::caution[Graceful stops are a Unix guarantee]
Stopping a child is a `SIGTERM`, which it handles by finishing the tasks it
holds. Windows has no equivalent, so there the supervisor's stop is a hard kill
and whatever those workers were running is left unacknowledged. Nothing is lost:
another worker reclaims it after
[`idle_timeout_ms`](/reference/configuration/). ArdiQ's own CI covers Linux, and
`--workers` is tested there; on Windows, one worker per service is the safer
shape.
:::

## `ardiq dlq`

Inspect and replay the tasks in the [dead letter queue](/guides/dead-letters/).

```console
$ ardiq dlq list MODULE:ATTR [--limit N]
$ ardiq dlq replay MODULE:ATTR (ID... | --all)
$ ardiq dlq delete MODULE:ATTR (ID... | --all)
```

| Command | Description |
|---|---|
| `list` | Print dead tasks, newest first: id, task, when it failed (UTC), tries and error. `--limit`/`-n` caps how many (default: 50). |
| `replay` | Enqueue the given tasks again, with the same ids and a fresh retry budget. |
| `delete` | Drop the given tasks without running them. |

`replay` and `delete` take task ids or `--all`. With `--all`, a replay that fails again
while the command runs stays in the queue for the next one. An id that is not in the
dead letter queue is reported, and the command exits with status 1.

## `ardiq doctor`

Check that an app can reach and use its Redis.

```console
$ ardiq doctor MODULE:ATTR [--json]
ok    redis      redis://localhost:6379 (0.3 ms)
ok    version    7.4.1
ok    write      keys and Lua scripts allowed
ok    lane high  0 queued, 0 delayed, 0 in flight
ok    lane low   12 queued, 3 delayed, 4 in flight
ok    workers    2 live: 1f3a9c2e running=4, 8b2d7e01 running=0
```

| Check | Fails or warns when |
|---|---|
| `redis` | Fails if Redis cannot be reached within 5 seconds. Warns if a round trip takes over 100 ms, because every task costs several. A password in the URL is shown as `***`. |
| `version` | Fails below Redis 6.2, which added the `XAUTOCLAIM` that crash recovery relies on. |
| `write` | Fails if the user may not write a key or run a Lua script, which is what a restrictive ACL usually breaks. The probe key is deleted straight away. |
| `lane NAME` | One per priority, showing tasks queued, delayed and in flight. Warns if workers are alive but none of them reads this lane: they were started with different `priorities`, so tasks here wait forever. |
| `workers` | Warns if no worker on this queue is alive. A worker shows up as soon as it starts and drops off when it stops cleanly, or one `idle_timeout_ms` after it dies. |

The command exits with status 1 if any check fails; warnings alone exit with 0. Run it
from a machine and environment where the app runs, since it uses the app's own
`redis_url`, queue and priorities. `--json` prints `{"ok": ..., "checks": [{"name",
"status", "detail"}, ...]}`.

## Signals

`ardiq run` installs handlers for **SIGINT** (`Ctrl-C`) and **SIGTERM** that call
`app.stop()`. The worker finishes the tasks it is running, returns the ones it had only
prefetched to the queue, and exits. A second signal exits at once, leaving any running
task to be reclaimed by another worker. With `--workers`, the supervisor passes each
signal on to every worker. See [Running a worker](/guides/worker/#graceful-shutdown).
