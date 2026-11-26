// `Horsies.create`: each check with its code, the order of the refusals, and the thread rule (the core's checks run on
// a main thread only; on a worker thread no addon loads).

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { z } from "zod";

import { createHorsies } from "../../../horsies/src/app.ts";
import type { HorsiesConfig } from "../../../horsies/src/app/create.ts";
import { appInternals } from "../../../horsies/src/app/internals.ts";
import { defineTask } from "../../../horsies/src/contract/public.ts";
import type { AppConfigCheck } from "../../../horsies/src/native/sends/addon.ts";
import { ClientDefect } from "../../../horsies/src/thrown-errors.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import { catalogRefusal, DATABASE_URL, fakeChecks, fakeEnvironment, type FakeEnvironment } from "./support.ts";

const ping = defineTask({ name: "ping", input: {}, output: h.string() });

function config(tasks: HorsiesConfig["tasks"]): HorsiesConfig {
  return { broker: { databaseUrl: DATABASE_URL }, tasks };
}

/** The codes and paths of a refused create. Throws when the create gives an app. */
function refusalsOf(input: HorsiesConfig, environment: FakeEnvironment): readonly (readonly [string, string])[] {
  const created = createHorsies(input, environment);

  assert.equal(created.ok, false, "the create is refused");

  return created.ok ? [] : created.error.refusals.map((refused) => [refused.code, refused.path] as const);
}

/** The first refusal message of a refused create. */
function messagesOf(input: HorsiesConfig): string {
  const created = createHorsies(input, fakeEnvironment());

  return created.ok ? "" : created.error.refusals[0].message;
}

describe("Horsies.create: the config", () => {
  test("an unset environment variable of a secret is HRS-203, and the core checks do not run", () => {
    const environment = fakeEnvironment();
    const input: HorsiesConfig = { broker: { databaseUrl: { kind: "fromEnv", name: "HORSIES_TEST_UNSET_URL" } }, tasks: [ping] };
    const created = createHorsies(input, environment);

    assert.equal(created.ok, false);
    assert.equal(created.ok ? "" : created.error.refusals[0].code, "HRS-203");
    assert.equal(created.ok ? "" : created.error.refusals[0].message.includes("HORSIES_TEST_UNSET_URL"), true);
    assert.equal(environment.loads.checks, 0);
  });

  test("a variable that is set gives the secret", () => {
    const environment = fakeEnvironment({ variables: { HORSIES_TEST_URL: "postgresql://127.0.0.1:55432/horsies" } });
    const input: HorsiesConfig = { broker: { databaseUrl: { kind: "fromEnv", name: "HORSIES_TEST_URL" } }, tasks: [ping] };

    assert.equal(createHorsies(input, environment).ok, true);
  });

  test("a core config refusal keeps its catalog code, at the path config", () => {
    const environment = fakeEnvironment({ checks: fakeChecks({ config: [catalogRefusal("HRS-201", "cluster_wide_cap must be >= 1")] }) });

    assert.deepEqual(refusalsOf(config([ping]), environment), [["HRS-201", "config"]]);
  });

  test("a config text that the core's reader refuses is a defect: the ClientDefect reaches the caller", () => {
    const defect = new ClientDefect({ subject: "checkAppConfig", rule: "the config text reads as the core's AppConfig" });

    const checks = {
      ...fakeChecks(),
      checkAppConfig: (_json: string): AppConfigCheck => {
        throw defect;
      },
    };

    assert.throws(() => createHorsies(config([ping]), fakeEnvironment({ checks })), (thrown) => thrown === defect);
  });

  test("a core refusal with a code that the catalog does not hold is a defect: TypeError", () => {
    const environment = fakeEnvironment({ checks: fakeChecks({ config: [catalogRefusal("HRS-999", "unknown")] }) });

    assert.throws(() => createHorsies(config([ping]), environment), TypeError);
  });
});

describe("Horsies.create: the contract list", () => {
  test("a second contract with a listed name is HRS-301", () => {
    const other = defineTask({ name: "ping", input: { n: h.int() }, output: h.string() });

    assert.deepEqual(refusalsOf(config([ping, other]), fakeEnvironment()), [["HRS-301", "tasks[1] ping"]]);
  });

  test("the same contract listed twice is HRS-301", () => {
    const created = createHorsies(config([ping, ping]), fakeEnvironment());

    assert.equal(created.ok, false);
    assert.equal(created.ok ? "" : created.error.refusals[0].code, "HRS-301");
    assert.equal(created.ok ? "" : created.error.refusals[0].message.includes("listed twice"), true);
  });

  test("a queue in the default queue mode is HRS-200", () => {
    const queued = defineTask({ name: "queued", queue: "standard", input: {}, output: h.null() });

    assert.deepEqual(refusalsOf(config([queued]), fakeEnvironment()), [["HRS-200", "tasks[0] queued"]]);
    assert.equal(
      messagesOf(config([queued])),
      'tasks[0] queued: the task names the queue "standard", and the default queue mode takes no queue',
    );
  });

  test("a refused config adds no queue refusal: the queue check reads the queue fields of the input", () => {
    const queued = defineTask({ name: "queued", queue: "urgent", input: {}, output: h.null() });

    const input: HorsiesConfig = {
      broker: { databaseUrl: { kind: "fromEnv", name: "UNSET_VARIABLE" } },
      queueMode: "custom",
      customQueues: [{ name: "urgent", priority: 1 }],
      tasks: [queued],
    };

    assert.deepEqual(
      refusalsOf(input, fakeEnvironment()).map(([code]) => code),
      ["HRS-203"],
    );
  });

  test("no queue in the custom queue mode is HRS-200", () => {
    const input: HorsiesConfig = {
      broker: { databaseUrl: DATABASE_URL },
      queueMode: "custom",
      customQueues: [{ name: "standard", priority: 10 }],
      tasks: [ping],
    };

    assert.deepEqual(refusalsOf(input, fakeEnvironment()), [["HRS-200", "tasks[0] ping"]]);
    assert.equal(messagesOf(input), "tasks[0] ping: the custom queue mode needs a queue on each task");
  });

  test("a queue that is not a custom queue is HRS-103", () => {
    const billing = defineTask({ name: "charge", queue: "billing", input: {}, output: h.null() });

    const input: HorsiesConfig = {
      broker: { databaseUrl: DATABASE_URL },
      queueMode: "custom",
      customQueues: [{ name: "standard", priority: 10 }],
      tasks: [billing],
    };

    assert.deepEqual(refusalsOf(input, fakeEnvironment()), [["HRS-103", "tasks[0] charge"]]);
  });

  test("a wire type that checkKind refuses is HRS-105", () => {
    const ratio = defineTask({ name: "ratio", input: { value: h.float().default(Number.NaN) }, output: h.null() });

    assert.deepEqual(refusalsOf(config([ratio]), fakeEnvironment()), [["HRS-105", "tasks[0] ratio"]]);
  });

  test("an empty task name is HRS-102, as in the Python client, with its help", () => {
    const unnamed = defineTask({ name: "", input: {}, output: h.null() });
    const created = createHorsies(config([unnamed]), fakeEnvironment());
    const refused = created.ok ? [] : created.error.refusals;

    assert.deepEqual(
      refused.map((each) => [each.code, each.message, each.helpText]),
      [["HRS-102", "tasks[0] : name: the task name is empty [TaskName]", ["give a task name that is not empty"]]],
    );
  });

  test("a zod form with no stored form is HRS-105, with the adapter's fix as its help", () => {
    const dated = defineTask({ name: "dated", input: { at: z.date() }, output: h.null() });
    const created = createHorsies(config([dated]), fakeEnvironment());
    const refused = created.ok ? [] : created.error.refusals;

    assert.deepEqual(
      refused.map((each) => [each.code, each.message, each.helpText]),
      [["HRS-105", "tasks[0] dated: input.at: zod type date is refused [DATE] [ZodSchema]", ["use hz.instant(): a Date holds milliseconds only"]]],
    );
  });
});

describe("Horsies.create: the core's checks of a contract", () => {
  test("a declared code that the core refuses keeps its code (HRS-112)", () => {
    const coded = defineTask({ name: "coded", input: {}, output: h.null(), errorCodes: ["bad code"] });
    const environment = fakeEnvironment({ checks: fakeChecks({ codes: [catalogRefusal("HRS-112", "not UPPER_SNAKE_CASE")] }) });

    assert.deepEqual(refusalsOf(config([coded]), environment), [["HRS-112", "tasks[0] coded"]]);
  });

  test("the help of a core refusal: its one line, or no line", () => {
    const withHelp = { ...catalogRefusal("HRS-109", "execution SHARED_ASYNC requires timeoutMs"), help: { kind: "help", line: "set timeoutMs" } } as const;
    const environment = fakeEnvironment({ checks: fakeChecks({ options: [withHelp], codes: [catalogRefusal("HRS-112", "bad code")] }) });
    const coded = defineTask({ name: "coded", input: {}, output: h.null(), errorCodes: ["BAD"] });
    const created = createHorsies(config([coded]), environment);

    assert.deepEqual(created.ok ? [] : created.error.refusals.map((refused) => [refused.code, refused.helpText]), [
      ["HRS-112", []],
      ["HRS-109", ["set timeoutMs"]],
    ]);
  });

  test("options that the core refuses keep their code (HRS-109)", () => {
    const environment = fakeEnvironment({ checks: fakeChecks({ options: [catalogRefusal("HRS-109", "timeout_ms is required")] }) });

    assert.deepEqual(refusalsOf(config([ping]), environment), [["HRS-109", "tasks[0] ping"]]);
  });

  test("the options facts of a contract: name, queue route, class, times, retry, codes", () => {
    const checks = fakeChecks();

    const charge = defineTask({
      name: "charge",
      input: {},
      output: h.null(),
      errorCodes: ["PROVIDER_DOWN"],
      execution: "SHARED_ASYNC",
      timeoutMs: 30_000,
      retryPolicy: { kind: "fixed", intervalsSeconds: [60, 300], jitter: false, autoRetryFor: ["PROVIDER_DOWN"] },
    });

    assert.equal(createHorsies(config([charge]), fakeEnvironment({ checks })).ok, true);
    assert.deepEqual(checks.optionsFacts, [
      {
        taskName: "charge",
        queueName: "default",
        execution: "SHARED_ASYNC",
        timeout: { kind: "set", milliseconds: 30_000 },
        cancelGrace: { kind: "notSet" },
        retry: { kind: "fixed", intervalsSeconds: [60, 300], jitter: false, autoRetryFor: ["PROVIDER_DOWN"] },
        declaredCodes: { kind: "declared", codes: ["PROVIDER_DOWN"] },
      },
    ]);
  });
});

describe("Horsies.create: order and threads", () => {
  test("several refusals in one result: the config, then the contracts in list order", () => {
    const queued = defineTask({ name: "queued", queue: "standard", input: {}, output: h.null() });
    const ratio = defineTask({ name: "ratio", input: { value: h.float().default(Number.NaN) }, output: h.null() });
    const environment = fakeEnvironment({ checks: fakeChecks({ config: [catalogRefusal("HRS-201", "cap")] }) });

    assert.deepEqual(refusalsOf(config([queued, ping, ratio]), environment), [
      ["HRS-201", "config"],
      ["HRS-200", "tasks[0] queued"],
      ["HRS-105", "tasks[2] ratio"],
    ]);
  });

  test("a core refusal of an earlier contract comes before a TypeScript refusal of a later one", () => {
    const queued = defineTask({ name: "queued", queue: "standard", input: {}, output: h.null() });
    const environment = fakeEnvironment({ checks: fakeChecks({ options: [catalogRefusal("HRS-109", "timeout_ms is required")] }) });

    assert.deepEqual(refusalsOf(config([ping, queued]), environment), [
      ["HRS-109", "tasks[0] ping"],
      ["HRS-200", "tasks[1] queued"],
    ]);
  });

  test("on a worker thread the core checks do not run, no addon loads, and the app has no core", () => {
    const environment = fakeEnvironment({ mainThread: false });
    const created = createHorsies(config([ping]), environment);

    assert.equal(created.ok, true);
    assert.equal(environment.loads.checks, 0);
    assert.equal(environment.loads.producers, 0);
    assert.equal(created.ok ? appInternals(created.value).core.kind : "", "noCore");
  });

  test("on a worker thread the TypeScript checks still run", () => {
    const queued = defineTask({ name: "queued", queue: "standard", input: {}, output: h.null() });

    assert.deepEqual(refusalsOf(config([queued]), fakeEnvironment({ mainThread: false })), [["HRS-200", "tasks[0] queued"]]);
  });

  test("on a main thread the core checks run once per config and per contract, and the app has the addon core", () => {
    const environment = fakeEnvironment();
    const created = createHorsies(config([ping]), environment);

    assert.equal(created.ok, true);
    assert.equal(environment.loads.checks, 1);
    assert.deepEqual(environment.checks.calls, { configs: 1, codes: 0, options: 1 });
    assert.equal(environment.loads.producers, 1);
    assert.equal(created.ok ? appInternals(created.value).core.kind : "", "addon");
  });
});
