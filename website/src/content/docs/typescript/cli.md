---
title: CLI Reference
summary: cli.run(app) runs the worker, the check, the monitoring web UI or the fleet metrics server of a TypeScript app from the app's entry file and serves its worker children; the horsies bin of the package installs the skill files and fetches the docs.
related: [quick-start/getting-started, workers/worker-setup, configuration/app-config, ../workers/worker-architecture, ../monitoring/web-ui-deployment, ../observability/fleet-collector]
tags: [typescript, cli, cli.run, worker, check, web, metrics, child mode, exit codes]
---

# CLI Reference

A TypeScript app has two command-line programs:

| Program | Commands | Why |
|---------|----------|-----|
| The app's entry file, through `cli.run(app)` | `worker`, `check`, `web`, `metrics`, and child mode | The implementations of the tasks are in the entry, and the worker starts the entry again for each child |
| The `horsies` bin of the package | `skills install`, `get-docs` | These commands need no app |

What a worker and a check do is on the shared pages: [Worker Architecture](/horsies/workers/worker-architecture/) and [Heartbeats & Recovery](/horsies/workers/heartbeats-recovery/).

## Run the App

End the entry file with `cli.run`:

```ts
import { cli, Horsies } from "horsies";

const created = Horsies.create({
  broker: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  workerProfiles: { default: { isolated: { count: 2 } } },
  tasks: [],
});

if (!created.ok) {
  process.exit(1);
}

process.exitCode = await cli.run(created.value);
```

Implement the tasks, provide the state and add the child start hooks before `cli.run`: see [Worker Setup](/horsies/typescript/workers/worker-setup/).

The contract of `cli.run(app: Horsies): Promise<number>`:

1. `cli.run` runs on the main thread of a process. A call on a `worker_threads` thread is a defect (`ClientDefect`).
2. Child mode first. The worker starts the entry file again for each child, with the arguments `--horsies-child <fd> <pid>` after the entry. `cli.run` reads them before any other argument and serves the child until the worker stops it. A child start with arguments that cannot be used ends with exit code 73.
3. Every line of the entry file before `cli.run` runs in each child too. Build the app there, and start nothing else: each child would start it again.
4. Any other process runs the command that the first argument after the entry names: `worker`, `check`, `web` or `metrics`. No command, or an unknown command, writes the usage line and ends with exit code 2.
5. `cli.run` ends the process itself, with the exit code of the command. An open handle of user code (a pool, a timer) does not keep a stopped command alive. Before the end, it waits at most 2,000 ms for the core log records that still wait to reach the log sink.
6. A command that throws ends with exit code 1. The command name, the error name and its message are written to stderr.

## Commands

| Command | Does | Python command |
|---------|------|----------------|
| `<entry> worker --profile NAME [--loglevel LEVEL] [--metrics-listen HOST:PORT]` | Starts a worker of one profile of `workerProfiles` | [`horsies worker`](/horsies/cli/#horsies-worker) |
| `<entry> check [--live] [--loglevel LEVEL]` | Checks the app without starting a service | [`horsies check`](/horsies/cli/#horsies-check) |
| `<entry> web [--host HOST] [--port PORT] [--auth none\|trusted-header] [--trusted-header NAME] [--enable-actions] [--trace-link TEMPLATE] [--loglevel LEVEL]` | Serves the monitoring web UI of the app | [`horsies web`](/horsies/cli/#horsies-web) |
| `<entry> metrics --listen HOST:PORT [--loglevel LEVEL]` | Serves the fleet metrics of the app's database at `GET /metrics` | [`horsies metrics`](/horsies/cli/#horsies-metrics) |

`<entry>` is the runtime and the entry file: `node main.ts` or `bun main.ts`. A flag takes its value as `--flag value` or `--flag=value`. An unknown flag, a flag with no value, a value given to a switch, and a positional argument are argument errors: exit code 2, with the usage line of the command.

### `worker`

```bash
node main.ts worker --profile default
node main.ts worker --profile batch --loglevel debug
node main.ts worker --profile default --metrics-listen 0.0.0.0:9464
```

| Flag | Default | Description |
|------|---------|-------------|
| `--profile NAME` | none | The profile of `workerProfiles` that holds the settings of the worker: children, recycling, pools, claim limits, drain timeout |
| `--loglevel LEVEL` | `INFO` | The level of the core records of the worker and of its children. See [Logs](#logs) |
| `--metrics-listen HOST:PORT` | none | Serve the worker metrics (`GET /metrics`) and the health probes (`GET /livez`, `GET /readyz`) on `HOST:PORT`, or `[IPV6]:PORT`. Without it the worker serves no endpoint. See [Metrics](/horsies/observability/metrics/) |

- A missing or unknown `--profile` exits with code 1, and nothing starts. The refusal lists the profile names of the app.
- A `--metrics-listen` value that the core refuses is an argument error: exit code 2. An address that does not bind does not stop the worker. See [When the Address Does Not Bind](/horsies/observability/metrics/#when-the-address-does-not-bind).
- The check of the app runs before the worker starts. A refused check writes its report on stderr and exits with code 1. The other causes of exit code 1 are on [Worker Setup](/horsies/typescript/workers/worker-setup/#run-a-worker).
- `SIGTERM` or `SIGINT` stops the worker. It drains within the drain timeout of the profile and exits with code 0. A later signal writes one line and changes nothing. See [Graceful Shutdown](/horsies/workers/worker-architecture/#graceful-shutdown).

### `check`

```bash
node main.ts check
node main.ts check --live
```

| Flag | Default | Description |
|------|---------|-------------|
| `--live` | off | After a passing static check, also connect to the database, and compare each worker profile with the connection limit of the server |
| `--loglevel LEVEL` | `WARNING` | See [Logs](#logs) |

The static check reads no database. It runs in phases and stops at the first phase that refuses:

1. The registrations after `Horsies.create`: an implementation of a contract that the app does not list, and a second implementation of one contract.
2. Each listed contract has an implementation in this entry (`HRS-300`). A check that passes means that a worker start from the same entry passes the same rule.
3. The codes of each implementation's `exceptions` map pass the code rules of the core.

A check that passes writes its text on stdout and exits with code 0:

```text
ok: all validations passed
  1 task(s) registered
  declared error codes: none
```

A refused check writes its report on stderr and exits with code 1:

```text
error[HRS-300]: tasks[0] greet: the contract has no implementation in this entry
  = help: call app.implement(contract, body) in a module that the entry imports before cli.run
error: aborting due to 1 previous errors
```

- `--live` installs no schema, and writes nothing but the one `NOTIFY` of its `LISTEN` probe. A failed connect, a failed `SELECT 1` and a refused profile are refusals of the report. A core of this process that refuses the live steps exits with code 1.
- Sends stay suppressed in the `check` command: a send at module level writes nothing.

### `web`

```bash
node main.ts web
node main.ts web --host 0.0.0.0 --auth trusted-header --enable-actions
node main.ts web --trace-link 'https://grafana.example.com/explore?traceId={trace_id}'
```

| Flag | Default | Description |
|------|---------|-------------|
| `--host HOST` | `127.0.0.1` | Bind address |
| `--port PORT` | `8600` | Bind port, a whole number from 0 to 65535; `0` asks the operating system for a free port |
| `--auth MODE` | `none` | `none` (a loopback host only) or `trusted-header` |
| `--trusted-header NAME` | `X-Forwarded-User` | The identity header that the proxy sets, in `trusted-header` mode |
| `--enable-actions` | off | Allow task cancel and workflow pause, resume and cancel. Without it the UI is view only |
| `--trace-link TEMPLATE` | none | The link of a task's trace id in the task detail: an `http` or `https` URL with one `{trace_id}`. The core checks the template; a template that breaks a rule exits 2 and names the rule |
| `--loglevel LEVEL` | `INFO` | See [Logs](#logs) |

- The server serves the app's own monitoring handler, with the app's database, contracts and workflows. To mount the handler in a server of the app, see [Web UI Mount](/horsies/typescript/monitoring/web-ui-mount/).
- The check of the app runs before the server starts. A refused check writes its report on stderr and exits with code 1.
- `--auth none` on a host that is not `localhost`, an IPv4 address in `127.0.0.0/8` or `::1` exits with code 2. A `--trusted-header` value that is not a header name exits with code 2. `--auth trusted-header` writes a warning: the proxy must strip or overwrite that header on each incoming request. The access rules are on the shared page: [Bind rules](/horsies/monitoring/web-ui-deployment/#bind-rules) and [Trusted-header mode](/horsies/monitoring/web-ui-deployment/#trusted-header-mode).
- An address that does not bind exits with code 1.
- When the server listens, it writes `Serving horsies monitoring UI on http://HOST:PORT` on stderr.
- `SIGTERM` or `SIGINT` closes the handler, then the server, and exits with code 0.

### `metrics`

```bash
node main.ts metrics --listen 0.0.0.0:9465
```

| Flag | Default | Description |
|------|---------|-------------|
| `--listen HOST:PORT` | required | The address to serve on: `HOST:PORT`, or `[IPV6]:PORT`. There is no default |
| `--loglevel LEVEL` | `INFO` | See [Logs](#logs) |

- The command serves the fleet metrics of the app's database at `GET /metrics`, in the Prometheus text format. What the metrics hold and when the command reads them: [Fleet Collector](/horsies/observability/fleet-collector/).
- The `queue` and `schedule` label values are the names of the app config; other names are `(unknown)`.
- The server reads through its own connection, with no schema step and no listener. A failed connect or a failed statement is a failure gauge in the text; it does not end the command.
- A missing `--listen`, or an address that the core refuses, is an argument error: exit code 2. The command exits with code 1 when the address does not bind, and with code 0 at `SIGTERM` or `SIGINT`.

## Logs

`--loglevel` takes `DEBUG`, `INFO`, `WARNING`, `ERROR` or `CRITICAL`, in any case. Another value is an argument error: exit code 2.

- The level is the default threshold of the core loggers of this process. The `worker` command gives its level to each child.
- `CRITICAL` forwards no core record: the core has no level above `ERROR`.
- The default sink writes one line per record on stderr. `configureLogging({ sink })` sends the records of this process to a function in place of stderr; the `--loglevel` of a command does not change the sink.

```ts
import { configureLogging } from "horsies";

configureLogging({
  sink: (records) => {
    for (const record of records) {
      process.stderr.write(`${record.time.toString()} ${record.level} ${record.logger}: ${record.message}\n`);
    }

    return undefined;
  },
});
```

The fields of the core's records are on [Logs](/horsies/observability/logs/).

## Exit Codes

The exit codes of `cli.run` in a process that runs a command:

| Code | Meaning |
|------|---------|
| 0 | The worker, the web server or the metrics server stopped at `SIGTERM` or `SIGINT`, or the check passed |
| 1 | Any other end: a refused check, a refused profile, a start that failed, a worker that failed, an address that does not bind, or a command that threw |
| 2 | Arguments that do not parse, no command, or an unknown command |
| 77 | `worker`: the worker could not write runner heartbeats, and it had not stopped 120 seconds after it started to stop. See [When the Worker Cannot Write Heartbeats](/horsies/workers/heartbeats-recovery/#when-the-worker-cannot-write-heartbeats) |
| 79 | `worker`: the worker held a task, and its liveness lock was not held for 60 seconds. See [Worker Liveness Lock](/horsies/workers/heartbeats-recovery/#worker-liveness-lock) |

A worker child ends with one of these codes. The worker reads the code to name how the child ended.

| Code | Meaning |
|------|---------|
| 0 | The worker sent its stop frame |
| 1 | Code outside every run threw, or child mode failed. The cause is on stderr |
| 71 | The worker is gone |
| 72 | The worker sent a malformed frame, or a frame over the bounds |
| 73 | The child mode arguments cannot be used |
| 75 | The child start failed, after a report that names the cause |
| 78 | The worker speaks another protocol version |
| 173 | A provider or a child start hook threw, rejected or ran past its bound |

## The `horsies` Bin

The package installs one bin, `horsies`. It runs with no app:

```bash
npx horsies skills install --target .claude/skills
npx horsies get-docs
```

| Command | Does | Python command |
|---------|------|----------------|
| `horsies skills install --target DIR [--force]` | Copies the agent skill folder of this release, which ships in the package, to `DIR/horsies/` | [`horsies skills install`](/horsies/cli/#horsies-skills-install) |
| `horsies get-docs [--output DIR] [--ref main]` | Fetches the docs of this release for AI agents into `DIR` (default `.horsies-docs`): the TypeScript pages, the shared pages and `llms.txt` | [`horsies get-docs`](/horsies/cli/#horsies-get-docs) |

`horsies --version` (or `-V`) prints `horsies <version>`. The outcomes and the exit codes of each command are those of the Python command that the table links. The worker, the check, the web UI and the metrics server of an app run from the app's entry file, not from this bin: the implementations of the tasks are in the entry. The bin refuses `worker`, `check`, `web` and `metrics` with exit code 2 and names the entry file as the place to run them.
