---
title: CLI
description: The ardiq command-line interface for running workers.
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

## Signals

`ardiq run` installs handlers for **SIGINT** (`Ctrl-C`) and **SIGTERM** that call
`app.stop()`, so the worker shuts down gracefully and lets in-flight tasks settle. See
[Running a worker](/guides/worker/#graceful-shutdown).
