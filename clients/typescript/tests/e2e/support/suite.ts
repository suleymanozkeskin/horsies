// One test of the suite: a new database, the app of an instance in the test process with its schema installed, a
// marker directory, and the worker processes of the worker entry (Rust `clients/rust/tests/worker/src/harness.rs`).
//
// The test process is a producer: it sends and reads through the client API. Task bodies run only in the children
// of the worker processes, which build the same app from the environment that `processEnv` gives them. Each test
// runs inside `e2eTest`, which bounds its wall time and ends the suite (the processes, the app, the database) also
// after a failure.

import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import { type AnyTaskContract, type AnyWorkflowContract, Horsies, type TaskHandle } from "#horsies";
import { appInternals } from "#horsies";
import { buildApp } from "../src/app.ts";
import { connectionEnv } from "../src/connection.ts";
import { type Instance, INSTANCE_ENV, instanceConfig, type Profile, readyClass, type ReadyClass, type ThreadProfile } from "../src/instances.ts";
import { MARKER_DIR_ENV, MarkerDir } from "../src/markers.ts";
import { customHealthcheck } from "../src/tasks/custom.ts";
import { recoveryHealthcheck } from "../src/tasks/recovery-queues.ts";
import { healthcheck } from "../src/tasks/basic.ts";
import { sharedHealthcheck } from "../src/tasks/shared.ts";
import { threadHealthcheck } from "../src/tasks/threads.ts";
import {
  createDatabase,
  dropDatabase,
  openDatabaseSession,
  type ServerSettings,
  serverSettings,
  settingsRefusedText,
  type TestDatabase,
} from "./database.ts";
import { postgresErrorText } from "./postgres/client.ts";
import { exitText, type ProcessCommand, TestProcess } from "./process.ts";
import { TaskRows } from "./rows.ts";

/** The default wall bound of one test: its database, its workers, its waits and its end. */
export const TEST_LIMIT_MS = 120_000;

/** The longest wait for a worker to log `WORKER_READY_LINE` and to run its readiness task. */
export const READY_LIMIT_MS = 60_000;

/** The line that the core worker logs once its start ended (`horsies-core` `worker/worker.rs`). */
export const WORKER_READY_LINE = "worker ready";

/** The result wait of one readiness probe. */
const READY_PROBE_MS = 500;

/** The log level of the worker processes. */
const LOG_LEVEL = "INFO";

/** The worker entry of the suite. */
export const WORKER_ENTRY: string = fileURLToPath(new URL("../src/main.ts", import.meta.url));

/** The settings of this process's test server, read at the first suite. */
let settingsOfRun: ServerSettings | undefined;

function runSettings(): ServerSettings {
  if (settingsOfRun === undefined) {
    const read = serverSettings((name) => process.env[name]);

    if (!read.ok) {
      assert.fail(settingsRefusedText(read.error));
    }

    settingsOfRun = read.value;
  }

  return settingsOfRun;
}

/** One end-to-end test: `body` gets a new suite of `instance`, ended after it in every case, within `limitMs`. */
export function e2eTest(name: string, instance: Instance, body: (suite: Suite) => Promise<void>, limitMs: number = TEST_LIMIT_MS): void {
  test(name, { timeout: limitMs }, async () => {
    const suite = await Suite.start(instance);

    try {
      await body(suite);
    } finally {
      await suite.end();
    }
  });
}

/** The resources of one test. */
export class Suite {
  readonly instance: Instance;
  readonly app: Horsies;
  readonly markers: MarkerDir;
  readonly db: TestDatabase;
  readonly rows: TaskRows;
  readonly #settings: ServerSettings;
  readonly #logDir: string;
  readonly #processes: TestProcess[] = [];
  readonly #apps: Horsies[] = [];
  readonly #closeRows: () => Promise<void>;

  private constructor(parts: SuiteParts) {
    this.instance = parts.instance;
    this.app = parts.app;
    this.markers = parts.markers;
    this.db = parts.db;
    this.rows = parts.rows;
    this.#settings = parts.settings;
    this.#logDir = parts.logDir;
    this.#closeRows = parts.closeRows;
    this.#apps.push(parts.app);
  }

  /** A new database, the app of `instance` on it with its schema installed, and a marker directory. */
  static async start(instance: Instance): Promise<Suite> {
    const settings = runSettings();
    const db = await createDatabase(settings);

    if (!db.ok) {
      assert.fail(`create the test database: ${postgresErrorText(db.error)}`);
    }

    const session = await openDatabaseSession(settings, db.value.name);

    if (!session.ok) {
      assert.fail(`connect to ${db.value.name}: ${postgresErrorText(session.error)}`);
    }

    const app = appOf(instance, db.value);

    await installSchema(app, db.value.name);

    return new Suite({
      instance,
      app,
      markers: MarkerDir.create(),
      db: db.value,
      rows: new TaskRows(session.value),
      settings,
      logDir: mkdtempSync(join(tmpdir(), "horsies-ts-e2e-logs-")),
      closeRows: async () => session.value.close(),
    });
  }

  /** The environment of a process of the worker entry. */
  processEnv(): Readonly<Record<string, string>> {
    return { ...connectionEnv(this.db.connection), [INSTANCE_ENV]: this.instance, [MARKER_DIR_ENV]: this.markers.path };
  }

  /** Start the worker entry with `args` and the extra environment `env`. Not waited for. */
  spawnEntry(label: string, args: readonly string[], env: Readonly<Record<string, string>> = {}): TestProcess {
    const command: ProcessCommand = { program: process.execPath, args: [WORKER_ENTRY, ...args], env: { ...this.processEnv(), ...env } };
    const started = TestProcess.start(label, command, join(this.#logDir, `${String(this.#processes.length)}-${label.replaceAll(/\W/g, "_")}.log`));

    this.#processes.push(started);

    return started;
  }

  /** Start a worker of `profile` of `instance` (the suite's own by default) without waiting for it. */
  spawnWorker(profile: Profile | ThreadProfile, instance: Instance = this.instance): TestProcess {
    return this.spawnEntry(`worker ${profile}`, ["worker", "--profile", profile, "--log-level", LOG_LEVEL], { [INSTANCE_ENV]: instance });
  }

  /**
   * Start a worker of `profile` and wait until it logged `WORKER_READY_LINE` and a readiness task of the profile's
   * class ran. The log line is this worker's own: with two workers, the first can run the readiness task of the
   * second before the second is ready. A worker of another instance than the suite's must run the suite's readiness
   * task.
   */
  async worker(profile: Profile | ThreadProfile, instance: Instance = this.instance): Promise<TestProcess> {
    const worker = this.spawnWorker(profile, instance);

    await worker.waitForLog(WORKER_READY_LINE, READY_LIMIT_MS);
    await this.waitReady(worker, profile);

    return worker;
  }

  /** Wait until `worker`, of `profile`, ran a readiness task. Fails the test when it exits first or the limit passes. */
  async waitReady(worker: TestProcess, profile: Profile | ThreadProfile): Promise<void> {
    const handle = await this.#sendReadiness(profile);

    await worker.waitUntil(READY_LIMIT_MS, "the worker runs its readiness task", async () => {
      const read = await handle.get({ waitMs: READY_PROBE_MS });

      return read.ok && read.value.kind === "succeeded";
    });
  }

  async #sendReadiness(profile: Profile | ThreadProfile): Promise<TaskHandle<string, string>> {
    const sent = await this.app.send(instanceReadinessTask(this.instance, profile), {});

    if (!sent.ok) {
      assert.fail(`send the readiness task: ${sent.error.code}: ${sent.error.message}`);
    }

    return sent.value;
  }

  /** A second app of the suite's instance on the same database, with `tasks` and `workflows` as its contract lists. */
  otherApp(tasks: readonly AnyTaskContract[], workflows: readonly AnyWorkflowContract[] = []): Horsies {
    const created = Horsies.create({ ...instanceConfig(this.instance, this.db.connection), tasks, workflows });

    if (!created.ok) {
      assert.fail(`create a second app: ${created.error.refusals.map((refused) => refused.message).join("; ")}`);
    }

    this.#apps.push(created.value);

    return created.value;
  }

  /** Stop every process, close the apps and the row session, drop the database, remove the markers and the logs. */
  async end(): Promise<void> {
    for (const started of this.#processes.toReversed()) {
      const end = await started.stop();

      if (end.kind === "killedAfterStopWait") {
        process.stderr.write(`suite end: ${started.label} did not exit after SIGTERM (${exitText(end.exit)}); its group was killed\n`);
      }
    }

    for (const app of this.#apps) {
      await closeApp(app);
    }

    await this.#closeRows();

    const dropped = await dropDatabase(this.#settings, this.db.name);

    if (!dropped.ok) {
      process.stderr.write(`suite end: the database ${this.db.name} was not dropped: ${postgresErrorText(dropped.error)}\n`);
    }

    this.markers.remove();
    rmSync(this.#logDir, { recursive: true, force: true });
  }
}

/** The parts of a suite that `start` builds. */
type SuiteParts = {
  readonly instance: Instance;
  readonly app: Horsies;
  readonly markers: MarkerDir;
  readonly db: TestDatabase;
  readonly rows: TaskRows;
  readonly settings: ServerSettings;
  readonly logDir: string;
  readonly closeRows: () => Promise<void>;
};

/** The readiness task of a worker of `profile` of `instance`: a task of the instance that a child of the profile runs. Pure. */
function instanceReadinessTask(
  instance: Instance,
  profile: Profile | ThreadProfile,
): typeof customHealthcheck | typeof recoveryHealthcheck | ReturnType<typeof readinessTask> {
  switch (instance) {
    case "custom_queues":
      return customHealthcheck;
    case "recovery_queues":
      return recoveryHealthcheck;
    case "default":
    case "cluster_cap":
    case "recovery":
    case "soft_cap":
    case "queue_retention":
    case "declared_retention":
    case "hook_throws":
    case "provider_throws":
    case "hook_past_bound":
    case "thread_profiles":
    case "scheduler":
    case "scheduler_invalid_rules":
    case "scheduler_invalid_task":
    case "handlers":
    case "handler_hook_throws":
    case "handler_hook_past_bound":
    case "worker_ended_spans":
      return readinessTask(readyClass(profile));
  }
}

/** The readiness task of a class in an instance of the default tasks. Pure. */
function readinessTask(readiness: ReadyClass): typeof healthcheck | typeof sharedHealthcheck | typeof threadHealthcheck {
  switch (readiness) {
    case "ISOLATED":
      return healthcheck;
    case "SHARED_ASYNC":
      return sharedHealthcheck;
    case "SHARED_THREAD":
      return threadHealthcheck;
  }
}

function appOf(instance: Instance, db: TestDatabase): Horsies {
  const built = buildApp(instance, db.connection);

  if (!built.ok) {
    assert.fail(`build the app of ${instance}: ${built.error}`);
  }

  return built.value;
}

/** Install the schema through the app's core handle, before any worker starts (Rust: the first `ping_database`). */
async function installSchema(app: Horsies, databaseName: string): Promise<void> {
  const core = appInternals(app).core;

  if (core.kind !== "addon") {
    assert.fail(`the app of the test process has no addon core (${core.kind})`);
  }

  const installed = await core.handle.installSchema();

  switch (installed.kind) {
    case "installed":
      return;
    case "notRun":
      assert.fail(`install the schema in ${databaseName}: the schema mode of the app runs no install`);
    case "failed":
      assert.fail(`install the schema in ${databaseName}: ${installed.error.class}: ${installed.error.slots.map((slot) => slot.name).join(", ")}`);
  }
}

/** Close the core pools of `app`, so a later test does not hold its connections. */
async function closeApp(app: Horsies): Promise<void> {
  const core = appInternals(app).core;

  if (core.kind === "addon") {
    await core.handle.close();
  }
}
