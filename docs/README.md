# ArdiQ documentation

The documentation site for [ArdiQ](https://github.com/17tayyy/ardiq), built with
[Astro](https://astro.build) + [Starlight](https://starlight.astro.build).

## Commands

Run these from this `docs/` directory:

| Command           | Action                                          |
| ----------------- | ----------------------------------------------- |
| `npm install`     | Install dependencies                            |
| `npm run dev`     | Start the dev server at `localhost:4321`        |
| `npm run build`   | Build the production site to `./dist/`          |
| `npm run preview` | Preview the build locally                       |

## Structure

- `src/content/docs/` — the pages (Markdown / MDX).
  - `guides/` — narrative guides (introduction, getting started, performance, tasks, enqueuing, results, worker, serialization).
  - `reference/` — configuration, Python API, and CLI reference.
- `astro.config.mjs` — site title, sidebar, and social links.
