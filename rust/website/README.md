# Horsies Documentation Site

Documentation site for [Horsies](https://github.com/suleymanozkeskin/horsies), built with [Astro](https://astro.build) + [Starlight](https://starlight.astro.build).

## Local Development

```
bun install
bun dev
```

The dev server runs at `localhost:4321`.

## Documentation Structure

Documentation pages live in `src/content/docs/` as `.md` or `.mdx` files. Each file maps to a route based on its path.

| Directory | Content |
| :--- | :--- |
| `quick-start/` | Getting started, configuration, producing tasks, workflows, scheduling |
| `concepts/` | Architecture, task lifecycle, result handling, queue modes, workflows |
| `tasks/` | Defining tasks, sending, error handling, results, retry policy |
| `configuration/` | App, broker, and recovery configuration |
| `workers/` | Worker architecture, concurrency, heartbeats, recovery |
| `scheduling/` | Scheduler overview, patterns, configuration |
| `cli/` | CLI reference |
| `internals/` | PostgreSQL broker, LISTEN/NOTIFY, database schema, serialization |

## Adding Pages

Create a new `.md` file in the appropriate `src/content/docs/` subdirectory. Starlight picks it up automatically. To add it to the sidebar, update the `sidebar` config in `astro.config.mjs`.

## Build

```
bun build
```

Output goes to `./dist/`.
