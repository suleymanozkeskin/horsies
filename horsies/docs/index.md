---
title: Horsies
summary: A PostgreSQL-backed background task queue and workflow engine for Python.
related: [cli]
tags: [index, navigation]
---

## About

Horsies is a distributed task queue library and comes with DAG like workflow support.

- **PostgreSQL as the only dependency** — no Redis, no RabbitMQ
- **Type-safe tasks** — tasks return results for explicit error handling
- **Real-time dispatch** — PostgreSQL LISTEN/NOTIFY, no polling
- **Scheduled tasks** — type-safe and human-readable patterns
- **Workflow Engine** — persistent workflow definitions for grouping tasks
- **Multi-queue support** — priority-based queues with per-queue concurrency limits

```text

⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣤⣶⣾⣿⣿⣿⣿⣷⣯⡀⠀⠀⠀⠀⠀⠀
⠀ ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣤⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣟⣿⣆⠀⠀⠀⠀
⠀ ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣴⣿⣿⣿⣿⣿⣿⣿⣿⡟⠹⣿⣿⣿⣿⣿⣦⡀⠀⠀⠀
  ⠀⠀⠀⠀⠀⠀⠀⠀⢀⣀⣀⣀⣠⣴⣾⣿⣿⣿⣶⣶⣶⣤⣤⣤⣤⣤⣶⣾⣿⣿⣿⣿⣿⣿⣿⡟⠀⠀⠈⠉⠛⠻⡿⣿⣿⠂⠀
⠀⠀⢀⣀⠀⢀⣀⣠⣶⣿⣿⠟⢛⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡟⡐⠀⠀⠀⠀⠀⠀⠈⠋⡿⠁⠀⠀
⠀⠀⠀⢹⣿⣿⣿⣿⣿⡿⠁⠀⢸⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣯⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠛⠻⠿⠛⠉⠀⠀⠀⠈⢯⡻⣿⣿⣿⣿⣿⢿⣿⣿⣿⣿⣿⣿⡿⣿⣿⣿⣿⣿⣿⣿⣿⡿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
  ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠻⣷⣿⣿⣿⡟⠀⠙⠻⠿⠿⣿⣿⠃⣿⣿⣿⣿⣿⣿⣿⣿⡁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
  ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⣿⣿⡿⠁⢀⠀⠀⠀⠀⠀⠂⠀⢿⣿⣿⣿⡍⠈⢁⣙⣿⢦⣄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
  ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢰⣿⣿⠏⠀⠀⣼⠀⠀⠀⠀⠀⠀⠀⠀⠙⢿⣿⣷⠀⠀⠀⠀⠁⣿⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠻⣿⣧⣀⢄⣿⡀⠀⠀⠀⠀⠀⠀⠀⠀⠈⢻⣿⣧⠀⠀⢀⣼⡟⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⣀⠀⠀⠀⢀⡀⠀⠀⠀⡀⠀⠀⠀⣀⠛⣿⣷⣍⠛⣦⡀⠀⠂⢠⣷⣶⣾⡿⠟⠛⢃⠀⢠⣾⡟⠀⠀⠀⠀⡀⠀⠀⠀⡀⠀⠀⠀
⠄⡤⠦⠤⠤⢤⡤⠡⠤⠤⢤⠬⠤⠤⠤⢤⠅⠀⠤⣿⣷⠄⠎⢻⣤⡦⠄⠀⠤⢵⠄⠠⠤⠬⣾⠏⠁⠀⠥⡤⠄⠠⠬⢦⡤⠀⠠⠵⢤⠠
⠚⠒⠒⠒⡶⠚⠒⠒⠒⡰⠓⠒⠒⠒⢲⠓⠒⠒⠒⢻⠿⠀⠀⠚⢿⡷⠐⠒⠒⠚⡆⠒⠒⠒⠚⣖⠒⠒⠒⠚⢶⠒⠒⠒⠚⢶⠂⠐⠒⠛

    __                    _
   / /_  ____  __________(_)__  _____
  / __ \/ __ \/ ___/ ___/ / _ \/ ___/      v0.1.0a19
 / / / / /_/ / /  (__  ) /  __(__  )       distributed task queue
/_/ /_/\____/_/  /____/_/\___/____/

[config]
  .> app:         Horsies
  .> role:        worker
  .> queue_mode:  CUSTOM
  .> queues:      high, normal, low
  .> broker:      postgresql+psycopg://postgres:****@localhost:5432/horsies
  .> cap:         50 (cluster-wide)

[tasks] (9 registered)
  . high_priority_compute
  . low_priority_compute
  . normal_priority_compute
```

## Navigation

- [CLI Reference](cli.md)
- [Documentation Standard](DOCUMENTATION_STANDARD.md)

Full end-user docs live in `website/src/content/docs`.
