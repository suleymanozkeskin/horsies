// The `worker` area of the addon with a built addon: the listen address check, the preparation of a worker for each
// of its outcomes, a stop request before a run, the log backlog, and the live check on a database. No worker runs:
// a run starts task children, which the end-to-end tests cover.
//
// Runs only where a built addon exists (`HORSIES_NATIVE_ADDON_PATH`) and `HORSIES_TEST_DATABASE_URL` names a
// throwaway PostgreSQL database. Run it under the memory watchdog (2 GiB, with a wall-time bound).

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { buildCoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import { coreState } from "../../../horsies/src/native/process-core/process-core.ts";
import {
  checkLive,
  parseListenAddress,
  prepareWorker,
  requestWorkerStop,
  workerLogBacklog,
} from "../../../horsies/src/native/worker/worker.ts";
import { ClientDefect } from "../../../horsies/src/thrown-errors.ts";
import { manifestJson, testDatabaseUrl } from "./manifest.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/** A database URL whose connect is refused at once: nothing listens on port 1. */
const REFUSED_DATABASE_URL = "postgresql://horsies@127.0.0.1:1/none";

/** The core's config text of an app with one profile `io` of one isolated child, on `databaseUrl`. */
function configText(databaseUrl: string): string {
  return JSON.stringify({
    database: { database_url: databaseUrl },
    worker_profiles: { io: { isolated: { kind: "isolated_children", count: 1 } } },
  });
}

/** The start members of a worker of `profile`, in the manifest's own forms. */
function startText(profile: object): string {
  return JSON.stringify({
    profile,
    metrics_endpoint: { kind: "NO_METRICS_ENDPOINT" },
    tasks: { ping: { execution: "ISOLATED", error_codes: [] } },
    handlers: { kind: "NO_HANDLERS" },
    worker_ended_spans: "NO_SPANS",
    child_launch: {
      command: { program: process.execPath, args: ["/app/main.js", "--horsies-child"] },
      preload: { kind: "NO_PRELOAD_PROGRAM" },
      program: { kind: "RUST" },
      log_level: "INFO",
    },
  });
}

describe("the worker area of the addon", () => {
  test("a host and port is an address; a text with no port is the core's refusal", () => {
    assert.deepEqual(parseListenAddress("0.0.0.0:9464"), { kind: "address", host: "0.0.0.0", port: 9464 });
    assert.equal(parseListenAddress("localhost").kind, "refused");
    assert.equal(coreState(), "not_started");
  });

  test("each preparation outcome; the core does not start", () => {
    const config = configText("postgresql://127.0.0.1:55432/horsies");

    assert.equal(prepareWorker(config, startText({ kind: "NAMED", name: "io" }), "CAPPED").kind, "prepared");
    assert.equal(prepareWorker(config, startText({ kind: "NAMED", name: "absent" }), "CAPPED").kind, "profileRefused");
    assert.equal(prepareWorker(config, startText({ kind: "NOT_GIVEN" }), "CAPPED").kind, "profileRefused");

    const pooled = JSON.stringify({
      database: { database_url: "postgresql://127.0.0.1:6432/horsies", pgbouncer_transaction_mode: true },
      worker_profiles: { io: { isolated: { kind: "isolated_children", count: 1 } } },
    });

    // The config check runs the broker check of the plan's connection step, so it refuses this text first.
    assert.throws(() => prepareWorker(pooled, startText({ kind: "NAMED", name: "io" }), "CAPPED"), ClientDefect);

    // The config check compares the URLs as the manifest writes them (`postgres://` as `postgresql://`), so it
    // refuses one URL named twice before the plan's connection step.
    const sameSession = JSON.stringify({
      database: {
        database_url: "postgres://127.0.0.1:6432/horsies",
        session_database: { kind: "SEPARATE", url: "postgresql://127.0.0.1:6432/horsies" },
        pgbouncer_transaction_mode: true,
      },
      worker_profiles: { io: { isolated: { kind: "isolated_children", count: 1 } } },
    });

    assert.throws(() => prepareWorker(sameSession, startText({ kind: "NAMED", name: "io" }), "CAPPED"), ClientDefect);

    const capped = JSON.stringify({
      database: { database_url: "postgresql://127.0.0.1:55432/horsies" },
      cluster_wide_cap: { kind: "cluster_cap", tasks: 4_294_967_295 },
      worker_profiles: { io: { isolated: { kind: "isolated_children", count: 1 } } },
    });

    const bound = prepareWorker(capped, startText({ kind: "NAMED", name: "io" }), "CAPPED");

    assert.equal(bound.kind === "manifestBound" ? bound.path : bound.kind, "clusterWideCap.tasks");

    const threads = JSON.stringify({
      database: { database_url: "postgresql://127.0.0.1:55432/horsies" },
      worker_profiles: {
        cpu: {
          isolated: { kind: "isolated_children", count: 1 },
          shared_thread: { kind: "shared_thread_children", count: 1, seats: 4 },
        },
      },
    });

    // A SHARED_THREAD profile starts where a worker thread has a memory cap (Node), and is refused where it has none
    // (Bun).
    assert.equal(prepareWorker(threads, startText({ kind: "NAMED", name: "cpu" }), "CAPPED").kind, "prepared");
    assert.deepEqual(prepareWorker(threads, startText({ kind: "NAMED", name: "cpu" }), "NOT_CAPPED"), {
      kind: "sharedThreadUnavailable",
      profile: "cpu",
    });
    assert.equal(coreState(), "not_started");
  });

  test("the core's parser accepts each form of the metrics endpoint, the handler list, the spans switch and the task facts", () => {
    const config = configText("postgresql://127.0.0.1:55432/horsies");
    const base: Record<string, unknown> = JSON.parse(startText({ kind: "NAMED", name: "io" }));

    const forms: readonly (readonly [string, unknown])[] = [
      ["metrics_endpoint", { kind: "LISTEN_ADDRESS", host: "127.0.0.1", port: 9464 }],
      ["metrics_endpoint", { kind: "NO_METRICS_ENDPOINT" }],
      ["handlers", { kind: "HANDLERS", handlers: [{ name: "alerts", event_type: "AttemptEnded" }] }],
      ["handlers", { kind: "NO_HANDLERS" }],
      ["worker_ended_spans", "MAKE_SPANS"],
      ["worker_ended_spans", "NO_SPANS"],
      ["tasks", { ping: { execution: "SHARED_ASYNC", error_codes: ["CARD_DECLINED"] } }],
      ["tasks", { ping: { execution: "SHARED_THREAD", error_codes: [] } }],
    ];

    for (const [member, form] of forms) {
      const prepared = prepareWorker(config, JSON.stringify({ ...base, [member]: form }), "CAPPED");

      assert.equal(prepared.kind, "prepared", `${member} ${JSON.stringify(form)}`);
    }
  });

  test("a start member that the core's parser refuses throws ManifestRefused", () => {
    const config = configText("postgresql://127.0.0.1:55432/horsies");
    const start = startText({ kind: "NAMED", name: "io" }).replace('"ISOLATED"', '"FORKED"');

    assert.throws(() => prepareWorker(config, start, "CAPPED"), { name: "ManifestRefused" });
  });

  test("a stop request before the run returns at once", () => {
    const prepared = prepareWorker(
      configText("postgresql://127.0.0.1:55432/horsies"),
      startText({ kind: "NAMED", name: "io" }),
      "CAPPED",
    );

    assert.equal(prepared.kind, "prepared");

    if (prepared.kind === "prepared") {
      requestWorkerStop(prepared.worker);
      requestWorkerStop(prepared.worker);
    }
  });

  test("the log backlog is one of its states", () => {
    assert.ok(["empty", "waiting", "noForwarder"].includes(workerLogBacklog()));
  });

  test("the live check passes on the test database", async () => {
    const url = testDatabaseUrl();
    const handle = buildCoreHandle(manifestJson(url, false), "PRODUCER");

    assert.deepEqual(await checkLive(handle, configText(url)), { kind: "passed" });
    await handle.close();
  });

  test("a refused connect is the connectivity step CONNECT or MAIN_URL", async () => {
    const handle = buildCoreHandle(manifestJson(REFUSED_DATABASE_URL, false), "PRODUCER");
    const outcome = await checkLive(handle, configText(REFUSED_DATABASE_URL));

    assert.equal(outcome.kind, "connectivityRefused");

    if (outcome.kind === "connectivityRefused") {
      assert.ok(["CONNECT", "MAIN_URL"].includes(outcome.step), outcome.step);
    }

    await handle.close();
  });
});
