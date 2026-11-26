---
title: Getting Started
summary: Install the horsies package, define a task contract, build the app in one entry file that is the worker and the producer, run a worker, and send the task.
related: [../installation, ../tasks/defining-tasks, ../configuration/app-config, ../workers/worker-setup, ../cli, ../tasks/sending-tasks]
tags: [typescript, quick start, install, first task, worker]
---

# Getting Started

One entry file of a TypeScript app is the producer, the worker and each child of the worker. The entry builds the app, implements the tasks, and hands the command line to `cli.run`, which runs the worker, the check, the web UI or the metrics server. This page builds an app with one task, runs a worker, and sends the task.

## Prerequisites

- Node 26 or later, or Bun 1.4 or later. See [Installation](/horsies/typescript/installation/).
- A PostgreSQL database for the app. A database belongs to one client: the core refuses a database that another client installed.

## Install the Package

```bash
npm install horsies
```

```bash
bun add horsies
```

The package is an ES module. Set `"type": "module"` in the `package.json` of the project.

## Write the App

`main.ts`:

```ts
import { cli, defineTask, h, Horsies } from "horsies";

const greet = defineTask({
  name: "greet",
  input: { name: h.string() },
  output: h.string(),
});

// Every process builds the same app: the producer, the worker and each child.
const created = Horsies.create({
  broker: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  workerProfiles: { default: { isolated: { count: 2 } } },
  tasks: [greet],
});

if (!created.ok) {
  for (const refusal of created.error.refusals) {
    console.error(`error[${refusal.code}]: ${refusal.message}`);
  }

  process.exit(1);
}

const app = created.value;

app.implement(greet, ({ name }) => greet.ok(`hello, ${name}`));

async function sendGreeting(): Promise<number> {
  const sent = await app.send(greet, { name: "world" });

  if (!sent.ok) {
    console.error(`greet was not sent: ${sent.error.code}: ${sent.error.message}`);

    return 1;
  }

  const read = await sent.value.get({ timeoutMs: 30_000 });

  if (!read.ok) {
    console.error(`greet has no outcome: ${read.error.kind}`);

    return 1;
  }

  const outcome = read.value;

  switch (outcome.kind) {
    case "succeeded":
      console.log(outcome.value);

      return 0;
    case "failed":
      console.error(`greet failed: ${outcome.error.code.code}`);

      return 1;
  }
}

process.exitCode = process.argv[2] === "send" ? await sendGreeting() : await cli.run(app);
```

- `defineTask` defines the contract of the task `greet`: its name, its input and its output. The parameter `name` is stored under its name: the input `{"name": "world"}`. See [Defining Tasks](/horsies/typescript/tasks/defining-tasks/).
- `Horsies.create` checks the config and each contract of `tasks`, and gives the app or every refusal. It opens no database connection: the app connects at its first operation and runs the schema step there. The database URL is read from the environment variable `DATABASE_URL` at `Horsies.create`. See [App Config](/horsies/typescript/configuration/app-config/).
- `app.implement` gives the contract its body. The body returns `greet.ok(value)` for a success. See [Defining Tasks](/horsies/typescript/tasks/defining-tasks/).
- The worker profile `default` has two isolated children. See [Worker Setup](/horsies/typescript/workers/worker-setup/).
- `app.send` gives a handle or the send error. `get` waits for the outcome. See [Sending Tasks](/horsies/typescript/tasks/sending-tasks/) and [Retrieving Results](/horsies/typescript/tasks/retrieving-results/).
- The worker starts this entry file again for each child, with arguments of its own. `cli.run` serves them. Every line before `cli.run` runs in each child too: build the app there and start nothing else.

## Run It

Start a worker of the profile `default`:

```bash
export DATABASE_URL=postgresql://app:secret@localhost:5432/horsies
node main.ts worker --profile default
```

With Bun:

```bash
bun main.ts worker --profile default
```

In a second terminal, with the same `DATABASE_URL`, send the task:

```bash
node main.ts send
```

```text
hello, world
```

`node main.ts check` checks the app without a database. The commands, their flags and their exit codes are on [CLI Reference](/horsies/typescript/cli/).

## Next Steps

| Topic | Page |
|-------|------|
| The `h.*` wire types and the zod adapter | [Wire Types](/horsies/typescript/tasks/wire-types/) |
| Contracts, execution classes, retries, the task runtime | [Defining Tasks](/horsies/typescript/tasks/defining-tasks/) |
| Errors of a task and their codes | [Task Errors](/horsies/typescript/tasks/errors/) |
| Workflows | [Workflow API](/horsies/typescript/concepts/workflows/workflow-api/) |
| An API server that sends tasks | [Frameworks](/horsies/typescript/guides/frameworks/) |
| Tests of task bodies | [Testing](/horsies/typescript/operations/testing/) |
