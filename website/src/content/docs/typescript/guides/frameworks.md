---
title: Frameworks
summary: Send tasks and read results from an HTTP server on Bun or Node that imports the contracts and the app, and keep the implementations and cli.run in the worker's entry file.
related: [../tasks/sending-tasks, ../tasks/retrieving-results, ../monitoring/web-ui-mount, ../cli, ../workers/worker-setup]
tags: [typescript, http, Bun.serve, node:http, web service, producer]
---

# Frameworks

An HTTP server (`Bun.serve`, `node:http`, or a framework on top of them) sends tasks and reads results through the app. It imports the task contracts and the app. It never imports the implementations: they and `cli.run` stay in the worker's entry file.

## Why and When

- A send needs the contract, listed in `tasks` of `Horsies.create`. It needs no implementation: the task body runs in a worker child.
- `Horsies.create` opens no database connection. Build the app once per process, at module load, then serve. The app connects at its first operation.
- The worker starts its entry file again for each child, and every line before `cli.run` runs in each child. A server in that file would start again in each child.
- A send, a read and a workflow start return a `Promise`. The event loop of the server stays free while they wait.

## Lay Out the Modules

| Module | Holds | The entry of |
|--------|-------|--------------|
| `contracts.ts` | The task contracts (`defineTask`) | No process |
| `app.ts` | `Horsies.create` with every contract | No process |
| `worker.ts` | `app.implement` for each contract, the providers and the start hooks, then `cli.run(app)` | The worker and each of its children |
| `server.ts` | The HTTP server, its sends and its reads | The server |

The imports name the `.ts` files, as Node and Bun run them. Set `allowImportingTsExtensions` (with `noEmit`) or `rewriteRelativeImportExtensions` in `tsconfig.json`, so the type check takes these imports.

`contracts.ts`:

```ts
import { defineTask, h } from "horsies";

export const sendReceipt = defineTask({
  name: "send_receipt",
  input: { orderId: h.uuid(), email: h.string() },
  output: h.null(),
  errorCodes: ["MAILBOX_FULL"],
});
```

`app.ts`:

<!-- ts-check: skip partial -- imports ./contracts.ts, the module of the block above -->
```ts
import { Horsies } from "horsies";

import { sendReceipt } from "./contracts.ts";

const created = Horsies.create({
  database: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  workerProfiles: { default: { isolated: { kind: "isolatedChildren", count: 2 } } },
  tasks: [sendReceipt],
});

if (!created.ok) {
  for (const refusal of created.error.refusals) {
    console.error(`error[${refusal.code}]: ${refusal.message}`);
  }

  process.exit(1);
}

export const app = created.value;
```

`worker.ts`, the entry of the worker:

<!-- ts-check: skip partial -- imports ./app.ts and ./contracts.ts, the modules of the blocks above -->
```ts
import { cli } from "horsies";

import { app } from "./app.ts";
import { sendReceipt } from "./contracts.ts";

app.implement(sendReceipt, async ({ orderId, email }, { ok }) => {
  console.log(`receipt of ${orderId} to ${email}`);

  return ok(null);
});

process.exitCode = await cli.run(app);
```

```bash
node worker.ts worker --profile default
```

## Send from a Handler

`server.ts` on Bun:

<!-- ts-check: skip partial -- imports ./app.ts and ./contracts.ts, the modules of the blocks above -->
```ts
import type { TaskSendError } from "horsies";

import { app } from "./app.ts";
import { sendReceipt } from "./contracts.ts";

/** The HTTP status of a send error. */
function sendErrorStatus(error: TaskSendError<null, "MAILBOX_FULL">): number {
  switch (error.code) {
    case "VALIDATION_FAILED":
    case "PAYLOAD_TOO_LARGE":
      return 400;
    case "ENQUEUE_FAILED":
      return error.retryable ? 503 : 500;
    case "SEAT_RELAY_FULL":
      return 503;
    case "IDEMPOTENCY_KEY_CONFLICT":
      return 409;
    case "PAYLOAD_MISMATCH":
    case "SEND_SUPPRESSED":
      return 500;
  }
}

Bun.serve({
  port: 8080,
  routes: {
    "/orders/:orderId/receipt": {
      POST: async (request) => {
        const body: { readonly email: string } = await request.json();
        const sent = await app.send(
          sendReceipt,
          { orderId: request.params.orderId, email: body.email },
          { idempotencyKey: `receipt:${request.params.orderId}` },
        );

        if (!sent.ok) {
          return Response.json({ code: sent.error.code, message: sent.error.message }, { status: sendErrorStatus(sent.error) });
        }

        return Response.json({ taskId: sent.value.taskId }, { status: 202 });
      },
    },
  },
});
```

- `app.send` gives a `Result`: the handle, or a send error of a closed union. The `switch` of `sendErrorStatus` names each code, so a code that the union gains makes the function fail typecheck. See [Sending Tasks](/horsies/typescript/tasks/sending-tasks/).
- With `idempotencyKey`, a repeat of the same send returns the handle of the first task. A repeat with another input is `IDEMPOTENCY_KEY_CONFLICT`. See [Atomic Enqueue and Topology](/horsies/operations/atomic-enqueue-and-topology/#make-the-send-repeatable-with-an-idempotency-key).
- An `orderId` that is not a UUID in the lower-case 8-4-4-4-12 form is `VALIDATION_FAILED`: the send checks the input with the contract's types and writes nothing.
- The server calls no `cli.run` and implements no task.

```bash
bun server.ts
```

## Read a Result by Its Id

A client that holds a task id reads the outcome later. `app.taskHandle(contract, taskId)` gives the handle of a stored id with no I/O. `TaskId.parse` checks the id text first.

`server.ts` on Node, with `node:http`:

<!-- ts-check: skip partial -- imports ./app.ts and ./contracts.ts, the modules of the blocks above -->
```ts
import { createServer } from "node:http";

import { TaskId } from "horsies";

import { app } from "./app.ts";
import { sendReceipt } from "./contracts.ts";

const RECEIPT_PATH = /^\/receipts\/([^/]+)$/u;

const server = createServer((request, response) => {
  const match = RECEIPT_PATH.exec(request.url ?? "");
  const taskId = TaskId.parse(match?.[1] ?? "");

  if (request.method !== "GET" || !taskId.ok) {
    response.writeHead(404).end();

    return;
  }

  void app
    .taskHandle(sendReceipt, taskId.value)
    .get({ waitMs: 0 })
    .then((read) => {
      if (!read.ok) {
        const status = read.error.kind === "waitTimeout" ? 202 : read.error.kind === "taskNotFound" ? 404 : 500;

        response.writeHead(status, { "content-type": "application/json" }).end(JSON.stringify({ state: read.error.kind }));

        return;
      }

      const outcome = read.value;
      const body = outcome.kind === "succeeded" ? { state: "sent" } : { state: "failed", code: outcome.error.code };

      response.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify(body));
    });
});

server.listen(8080);
```

- `get({ waitMs: 0 })` reads once and does not wait: a task that has not finished gives the read error `waitTimeout`. Do not hold a request open for a task: give the client the task id, and read again later.
- `taskHandle` takes the contract for the types only. The app need not list it.
- The read errors and the outcome are on [Retrieving Results](/horsies/typescript/tasks/retrieving-results/).

```bash
node server.ts
```

## Things to Avoid

**Do not start the server in the worker's entry file.** Each worker child runs the entry again and would start the server again. Keep the server in its own entry file.

**Do not import the implementations into the server.** The server needs the contracts only. A module that calls `app.implement` belongs to the worker's entry.

**Do not wait without a bound in a request handler.** `get()` with no `waitMs` waits until the task finishes. Pass a `waitMs`, or `0` for one read.

**Do not wait for a task result in a task body.** A read that waits in a process that runs task bodies gives `waitInsideTask` and reads nothing. Compose the tasks with a workflow: see [Workflow API](/horsies/typescript/concepts/workflows/workflow-api/).
