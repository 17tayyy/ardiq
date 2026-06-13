---
title: CLI
description: The ardiq command-line interface for running workers.
---

The `ardiq` command (a [Typer](https://typer.tiangolo.com/) app) ships in the **`cli`
extra** — the base `pip install ardiq` is library-only:

```console
$ pip install 'ardiq[cli]'
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

### Examples

```console
$ ardiq run example:app                # long-running worker
$ ardiq run example:app --burst        # drain the queue and exit
$ ardiq run myproject.worker:app -v    # verbose logging
```

## Signals

`ardiq run` installs handlers for **SIGINT** (`Ctrl-C`) and **SIGTERM** that call
`app.stop()`, so the worker shuts down gracefully and lets in-flight tasks settle. See
[Running a worker](/guides/worker/#graceful-shutdown).
