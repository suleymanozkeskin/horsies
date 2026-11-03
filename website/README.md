# Horsies Documentation Site

Documentation site for [Horsies](https://github.com/suleymanozkeskin/horsies), built with [Astro](https://astro.build) + [Starlight](https://starlight.astro.build).

## Local Development

```
bun install
bun run dev
```

The dev server runs at `localhost:4321`.

## Documentation Structure

Documentation pages live in `src/content/docs/` as `.md` or `.mdx` files. Each file maps to a route based on its path.

| Path | Content |
| :--- | :--- |
| `index.mdx` | Landing page |
| `quick-start/` | Getting started, configuration, producing tasks, workflows, scheduling, workflow patterns, the showcase |
| `concepts/` | Architecture, task lifecycle, result handling, queue modes, workflows |
| `tasks/` | Defining tasks, sending, errors and error handling, results, retry policy |
| `configuration/` | App, broker, recovery, retention, and remote Postgres configuration |
| `workers/` | Worker architecture, concurrency, heartbeats and recovery, child process hooks |
| `scheduling/` | Scheduler overview, patterns, configuration |
| `monitoring/` | Web UI, Syce, broker methods, action semantics, worker health |
| `operations/` | Deploying code changes, testing, atomic enqueue and topology, autovacuum tuning |
| `guides/` | Async frameworks, starting a workflow once |
| `migrations/` | Upgrade guide for each release that needs one |
| `troubleshooting/` | PgBouncer |
| `internals/` | Database schema, serialization, performance |
| `cli.md` | CLI reference |
| `questions-and-answers.md` | Questions and answers |
| `changelog.md` | Changelog |

## Adding Pages

Create a new `.md` file in the appropriate `src/content/docs/` subdirectory. Starlight picks it up automatically. To add it to the sidebar, update the `sidebar` config in `astro.config.mjs`.

## Build

```
bun run build
```

Output goes to `./dist/`.
