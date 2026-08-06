# Contributing to ArdiQ

Thanks for taking the time. Bug reports, docs fixes and features are all welcome.

## Getting set up

You need [uv](https://docs.astral.sh/uv/), a stable [Rust toolchain](https://rustup.rs)
and a Redis instance.

```bash
git clone https://github.com/17tayyy/ardiq
cd ardiq
docker compose up -d      # Redis on localhost:6379
uv sync --dev             # installs deps and builds the Rust extension
uv run pytest
```

The test suite needs a live Redis on `localhost:6379`. Everything runs against
database 0 under `ardiq:` keys.

### If you touch the Rust core

`uv run` does **not** notice changes to `core/src`. Rebuild explicitly:

```bash
uv sync --reinstall-package ardiq
```

Skipping this is the most common way to spend an hour debugging a change that
was never compiled in.

## Before you open a pull request

```bash
uv run ruff check .
uv run ruff format .
uv run ty check ardiq tests
uv run pytest
```

CI runs the same four on Python 3.12 and 3.13.

## How the project is laid out

```
core/src/       Rust: the worker loop, all Redis I/O, the Lua scripts
ardiq/          Python: the public API, task semantics, the wire format
tests/          pytest, one file per feature area
docs/           Astro + Starlight site (ardiq.bytay.dev)
```

The split is deliberate and worth respecting: **Rust is mechanism, Python is
policy**. The loop, the streams and the reclaim logic live in Rust; retries,
cron, serialization and anything a user can configure per task live in Python.
If a change needs new behaviour, prefer expressing it in Python first — cron and
abort both shipped with zero Rust changes.

## Changes that need a conversation first

Open an issue before writing code if your change:

- alters the public Python API (`Ardiq`, `Task`, `Job`, `TaskResult`, `TaskInfo`)
- changes the msgpack envelope or any Redis key layout
- adds a runtime dependency — `pip install ardiq` pulls in msgpack and nothing
  else, and keeping it that way is a feature

Everything else, just send the PR.

## Tests and docs

A behaviour change needs a test. A user-visible change needs both the README and
the matching page under `docs/src/content/docs/` updated in the same PR — the
docs site is built from this repo, so it goes stale in exactly one commit
otherwise.

Build the site locally with:

```bash
npm --prefix docs ci
npm --prefix docs run build
```

## Reporting bugs

Include your ArdiQ version, Python version, OS and Redis version, plus the
smallest snippet that reproduces it. A worker log with `--verbose` helps a lot.
