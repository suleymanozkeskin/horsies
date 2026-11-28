# horsies-core

The shared core of [horsies](https://github.com/suleymanozkeskin/horsies), a PostgreSQL-backed task queue and workflow engine. The Python client (the `horsies` package on PyPI), the Rust client (the `horsies` crate) and the TypeScript client (the `horsies` package on npm) run on this crate.

Do not depend on this crate in an application. Use a client: [`horsies`](https://crates.io/crates/horsies) for Rust, [`horsies`](https://pypi.org/project/horsies/) for Python, or [`horsies`](https://www.npmjs.com/package/horsies) for TypeScript. A client holds the typed task and workflow API, the configuration of an app and its registries. The public items of this crate are the interface between the core and its clients, not an API for applications.

The core holds the parts that every client shares:

- the PostgreSQL schema: the baseline SQL files in `baseline/`, installed by the core, and the statements that every write and read sends;
- the send path, the result reads and the task history;
- the worker parent: the claim loop, the outcome writes, the heartbeats, the recovery passes and the supervision of the child processes that run task bodies;
- the workflow engine: the start of a workflow tree from spec data, the node steps, the sub-workflow child starts and the recovery of workflows;
- the scheduler;
- the monitoring reads and actions, and, with the `web` feature, a monitoring handler that a host web framework mounts, with the built web UI in `webui-dist/`.

From 0.8.0, this crate, the Rust client, the Python package and the TypeScript package carry one version number.

Documentation: <https://suleymanozkeskin.github.io/horsies/>

License: MIT.
