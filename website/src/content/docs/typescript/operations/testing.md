---
title: Testing
summary: Run a task body with implementation.run and a TaskRuntime.forTest runtime, give it app state, check the app with Horsies.create and the check command, and suppress sends.
related: [../tasks/defining-tasks, ../configuration/app-config, ../workers/worker-setup, ../cli]
tags: [typescript, testing, unit test, TaskRuntime, forTest, implementation.run, HORSIES_SUPPRESS_SENDS]
---

# Testing

`app.implement` returns an `Implementation`. Its `run(input, runtime)` runs the body with no worker and no database, and checks the input and the output with the contract's types as a worker does. `TaskRuntime.forTest` gives the body a runtime with fixed answers. A test reads the `TaskOutcome` that `run` returns.

## Why and When

- Test the logic of a task body, its codes and its exception map with `implementation.run`.
- Test the app state that a body reads with the state entries of `TaskRuntime.forTest`.
- Test the config and the contract list with `Horsies.create`, and the app entry with the `check` command. Neither needs a database.
- Test the stored outcome, the retries, the deadline and the cancel with a worker on a test database.

## Run a Task Body

```ts
import assert from "node:assert/strict";
import { test } from "node:test";

import { AttemptNumber, defineTask, h, Horsies, TaskId, TaskRuntime } from "horsies";

const validateOrder = defineTask({
  name: "validate_order",
  input: { totalCents: h.int() },
  output: h.int(),
  errorCodes: ["LIMIT_EXCEEDED"],
});

const welcomeUser = defineTask({
  name: "welcome_user",
  input: { user: h.string() },
  output: h.string(),
});

const created = Horsies.create({
  database: { databaseUrl: { kind: "value", value: "postgresql://localhost/horsies_test" } },
  tasks: [validateOrder, welcomeUser],
});

if (!created.ok) {
  throw new Error(created.error.refusals.map((refusal) => `${refusal.code} ${refusal.path}: ${refusal.message}`).join("\n"));
}

const app = created.value;

const validate = app.implement(validateOrder, ({ totalCents }, { ok, err }) =>
  totalCents > 1_000_000 ? err({ code: "LIMIT_EXCEEDED", message: "order over the limit" }) : ok(totalCents),
);

const TASK_ID = TaskId.parse("0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b");
const FIRST_ATTEMPT = AttemptNumber.parse(1);

if (!TASK_ID.ok || !FIRST_ATTEMPT.ok) {
  throw new Error("the test task id or attempt number is refused");
}

test("an order over the limit is refused", async () => {
  const runtime = TaskRuntime.forTest({ taskId: TASK_ID.value, attempt: FIRST_ATTEMPT.value });
  const outcome = await validate.run({ totalCents: 2_000_000 }, runtime);

  assert.equal(outcome.kind, "failed");

  if (outcome.kind === "failed") {
    assert.equal(outcome.error.kind, "custom");
    assert.equal(outcome.error.code, "LIMIT_EXCEEDED");
  }
});
```

- `Horsies.create` opens no database connection: the test needs no database. On the main thread it loads the addon of the platform for the core's checks.
- `node:test` runs on Node (`node --test`) and on Bun (`bun test`).

`implementation.run(input, runtime)` returns `Promise<TaskOutcome<O, E>>`, the outcome as a task read gives it:

1. The input encodes with the contract's parameters and reads back, as a send does. A refusal is the outcome `failed` with the library code `WORKER_SERIALIZATION_ERROR`, and the body does not run.
2. The body runs with the decoded input, defaults filled, and `runtime`.
3. A value of `ok(...)` reads back with the output type; a refusal is `WORKER_SERIALIZATION_ERROR`. An error of `err(...)` is stored with its custom code. Any other return is `RETURN_TYPE_MISMATCH`. `ok` and `err` are members of the second argument of the body.
4. A thrown error of a class in the `exceptionMapper` option of `app.implement` is stored with its mapped code; any other thrown error is stored as `UNHANDLED_EXCEPTION`. Both carry the exception record.

A rule of the core on a custom code is not checked by `run`: that check needs a worker.

## TaskRuntime.forTest

`TaskRuntime.forTest(options)` builds a runtime with no worker, no database and no addon.

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `taskId` | `TaskId` | Yes | `runtime.taskId`, from `TaskId.parse` |
| `attempt` | `AttemptNumber` | Yes | The attempt number, from `AttemptNumber.parse`: a whole number from `AttemptNumber.MIN` (1) to `AttemptNumber.MAX` |
| `state` | entries | No | The app state of the test: `[[Key, value], ...]`, each a key that `app.provide` returned and a value of the key's type |
| `workflowContext` | stored workflow context | No | The stored form of the workflow context of a workflow node: `workflow_id`, `task_index`, `task_name` and `results_by_id`. Left out, the runtime holds `{ kind: "noContext" }`. A form that does not read throws `TypeError` |

The runtime answers:

| Member | In a test |
|--------|-----------|
| `taskId` | The `taskId` option |
| `fencingToken` | `{ taskId, value: attempt }` |
| `signal` | A signal that never fires |
| `cancelState()` | `{ kind: "notRequested" }` |
| `spawn(name, work)` | Runs `work` at once with a signal of its own that never fires. It is never refused |
| `state(key)` | The value of the entry for `key`. A key with no entry throws `ClientDefect` |

## Give a Body Its App State

`app.provide(factory)` returns a `StateKey`. In a worker, the factory runs in each task child before the child serves. In a test the factory does not run: the state entries of `TaskRuntime.forTest` give the value.

```ts
type Greeter = { readonly greeting: string };

const GreeterKey = app.provide((): Greeter => ({ greeting: "welcome" }));

const welcome = app.implement(welcomeUser, ({ user }, { ok, state }) =>
  ok(`${state(GreeterKey).greeting}, ${user}`),
);

test("welcome reads the greeter", async () => {
  const runtime = TaskRuntime.forTest({
    taskId: TASK_ID.value,
    attempt: FIRST_ATTEMPT.value,
    state: [[GreeterKey, { greeting: "hello" }]],
  });

  assert.deepEqual(await welcome.run({ user: "ada" }, runtime), { kind: "succeeded", value: "hello, ada" });
});
```

The typecheck refuses an entry whose value does not have the key's type.

## Check the App

`Horsies.create(config)` checks the config and every listed contract, and returns `CreateRefused` with every refusal. A test of the app's config asserts that it passes:

<!-- ts-check: skip partial -- appConfig is the config of the user's app -->
```ts
test("the app config and the contracts pass", () => {
  const checked = Horsies.create(appConfig);

  assert.ok(checked.ok, checked.ok ? "" : checked.error.refusals.map((refusal) => refusal.message).join("\n"));
});
```

Each `Refusal` has `code` (an `HRS-nnn` code), `path`, `message` and `help`. The `check` command of the app entry runs the checks of a worker start, the implementations included, with no database. See [CLI Reference](/horsies/typescript/cli/).

## Suppress Sends

A test that runs code which sends can suppress the sends of the process with the environment variable `HORSIES_SUPPRESS_SENDS=1`.

- The variable is read at each send. The whitespace around the value is ignored.
- A suppressed send returns `SEND_SUPPRESSED` and writes nothing.
- The check runs after the list check of the contract and before the options, the input and the database, so the test needs no database.
- The `check` command suppresses the sends of its process too.

See [Sending Tasks](/horsies/typescript/tasks/sending-tasks/#suppressed-sends).

## Things to Avoid

**Do not test the worker's behaviour with `implementation.run`.** A run has no deadline, no retry, no cancel and no stored outcome. Start a worker against a test database for those.

**Do not run a task with an `h.taskOutcome(contract)` parameter through `implementation.run`.** Such a parameter takes its value from a workflow node only. Test that body with a workflow.

**Do not run a test that sends against the database of a deployment.** Give the app of the test the URL of a test database, or suppress the sends.
