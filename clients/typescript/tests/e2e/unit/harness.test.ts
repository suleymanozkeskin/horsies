// The pure parts of the end-to-end harness: the server settings and database names, the connection of the worker
// entry, the runner's plan, the instances, the SQL literal and the marker files. No database and no process.

import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, test } from "node:test";

import { LAYER_FILES } from "../layers.ts";
import { fileCommand, runPlan } from "../run.ts";
import { connectionEnv, connectionFromEnv, DATABASE_URL_ENV, SESSION_DATABASE_URL_ENV } from "../src/connection.ts";
import { instanceConfig, parseInstance, readyClass, readyQueue } from "../src/instances.ts";
import { appendLine, createOnce, MARKER_DIR_ENV, MAX_ATTEMPTS, nextAttempt } from "../src/markers.ts";
import {
  databaseName,
  isRunDatabase,
  RUN_ID_VARIABLE,
  serverSettings,
  SERVER_URL_VARIABLE,
  TRANSACTION_POOL_URL_VARIABLE,
  urlWithDatabase,
  DATABASE_PREFIX_VARIABLE,
} from "../support/database.ts";
import { sqlText } from "../support/rows.ts";

const SERVER = "postgresql://postgres@127.0.0.1:55432/postgres";

/** A read of the variables of `values`. */
function variables(values: Readonly<Record<string, string>>): (name: string) => string | undefined {
  return (name) => values[name];
}

describe("the server settings", () => {
  test("an unset or empty server URL is refused: there is no default", () => {
    for (const values of [{}, { [SERVER_URL_VARIABLE]: "" }]) {
      const settings = serverSettings(variables(values));

      assert.equal(settings.ok ? "accepted" : settings.error.variable, SERVER_URL_VARIABLE);
    }
  });

  test("the defaults: the prefix horsies_test_, a random run id, a direct topology", () => {
    const settings = serverSettings(variables({ [SERVER_URL_VARIABLE]: SERVER }));

    assert.ok(settings.ok);
    assert.equal(settings.value.prefix, "horsies_test_");
    assert.match(settings.value.runId, /^[0-9a-f]{8}$/);
    assert.deepEqual(settings.value.topology, { kind: "direct" });
  });

  test("a prefix that is not a lower-case identifier ending in _ is refused", () => {
    for (const prefix of ["Upper_", "no_underscore", "9digit_", "a_prefix_far_longer_than_24_"]) {
      const settings = serverSettings(variables({ [SERVER_URL_VARIABLE]: SERVER, [DATABASE_PREFIX_VARIABLE]: prefix }));

      assert.equal(settings.ok ? "accepted" : settings.error.variable, DATABASE_PREFIX_VARIABLE, prefix);
    }
  });

  test("a run id that is not 8 lower-case hex digits is refused", () => {
    const settings = serverSettings(variables({ [SERVER_URL_VARIABLE]: SERVER, [RUN_ID_VARIABLE]: "XYZ" }));

    assert.equal(settings.ok ? "accepted" : settings.error.variable, RUN_ID_VARIABLE);
  });

  test("the transaction pool URL gives the transaction pool topology", () => {
    const pool = "postgresql://postgres@127.0.0.1:6432/postgres";
    const settings = serverSettings(variables({ [SERVER_URL_VARIABLE]: SERVER, [TRANSACTION_POOL_URL_VARIABLE]: pool }));

    assert.ok(settings.ok);
    assert.deepEqual(settings.value.topology, { kind: "transactionPool", poolUrl: pool });
  });

  test("a database name is the prefix, ts_, the run id and 24 hex digits; only such a name is of the run", () => {
    const settings = serverSettings(variables({ [SERVER_URL_VARIABLE]: SERVER, [RUN_ID_VARIABLE]: "0a1b2c3d" }));

    assert.ok(settings.ok);

    const name = databaseName(settings.value, "0123456789abcdef01234567");

    assert.equal(name, "horsies_test_ts_0a1b2c3d0123456789abcdef01234567");
    assert.ok(name.length <= 63);
    assert.equal(isRunDatabase(settings.value, name), true);
    assert.equal(isRunDatabase(settings.value, "horsies_test_ts_0a1b2c3d"), false);
    assert.equal(isRunDatabase(settings.value, "postgres"), false);
  });

  test("the database path of a URL is replaced and its query kept", () => {
    assert.deepEqual(urlWithDatabase("postgresql://postgres:secret@127.0.0.1:55432/postgres?sslmode=disable", "horsies_test_ts_00"), {
      ok: true,
      value: "postgresql://postgres:secret@127.0.0.1:55432/horsies_test_ts_00?sslmode=disable",
    });
  });
});

describe("the connection of the worker entry", () => {
  test("no database URL is refused", () => {
    assert.equal(connectionFromEnv(variables({})).ok, false);
  });

  test("a direct and a transaction pool connection go through the environment and back", () => {
    const direct = { kind: "direct", databaseUrl: SERVER } as const;
    const pooled = { kind: "transactionPool", poolUrl: "postgresql://postgres@127.0.0.1:6432/d", sessionUrl: SERVER } as const;

    assert.deepEqual(connectionEnv(pooled), { [DATABASE_URL_ENV]: pooled.poolUrl, [SESSION_DATABASE_URL_ENV]: SERVER });
    assert.deepEqual(connectionFromEnv(variables(connectionEnv(direct))), { ok: true, value: direct });
    assert.deepEqual(connectionFromEnv(variables(connectionEnv(pooled))), { ok: true, value: pooled });
  });
});

describe("the runner's plan", () => {
  test("the first argument is the runtime", () => {
    assert.equal(runPlan([]).ok, false);
    assert.equal(runPlan(["deno"]).ok, false);
  });

  test("an unknown layer file is refused", () => {
    assert.equal(runPlan(["node", "layer9-nothing"]).ok, false);
  });

  test("no file names: every layer file, in order; names: those files", () => {
    assert.deepEqual(runPlan(["bun"]), { ok: true, plan: { runtime: "bun", files: LAYER_FILES } });

    const filtered = runPlan(["node", "retention", "layer1-tasks"]);

    assert.ok(filtered.ok);
    assert.deepEqual(
      filtered.plan.files.map((file) => file.name),
      ["layer1-tasks", "retention"],
    );
  });

  test("each file runs under the watchdog with its bound, with the test runner of its runtime", () => {
    const file = { name: "retention", boundSeconds: 600 };
    const node = fileCommand("node", file);
    const bun = fileCommand("bun", file);

    assert.equal(node.program, "python3");
    assert.deepEqual(node.args.slice(1, 6), ["--max-mb", "2048", "--max-seconds", "600", "--"]);
    assert.deepEqual(node.args.slice(6), ["node", "--test", "--test-reporter=spec", "tests/e2e/layers/retention.ts"]);
    assert.deepEqual(bun.args.slice(6), ["bun", "test", "./tests/e2e/layers/retention.ts"]);
  });
});

describe("the instances", () => {
  test("an instance name parses; another text is refused", () => {
    assert.deepEqual(parseInstance("soft_cap"), { ok: true, value: "soft_cap" });
    assert.equal(parseInstance("nothing").ok, false);
  });

  test("a profile with only shared async children is made ready by a shared async task", () => {
    assert.equal(readyClass("shared-only-1x4"), "SHARED_ASYNC");
    assert.equal(readyClass("split-isolated-1-shared-1x4"), "ISOLATED");
    assert.equal(readyQueue("custom_queues"), "high");
    assert.equal(readyQueue("default"), "default");
  });

  test("a profile with only shared thread children is made ready by a shared thread task; a split one by an isolated task", () => {
    assert.equal(readyClass("thread-only-1x4"), "SHARED_THREAD");
    assert.equal(readyClass("split-isolated-1-thread-1x4"), "ISOLATED");
  });

  test("a transaction pool connection gives the PgBouncer mode with its session URL", () => {
    const config = instanceConfig("default", { kind: "transactionPool", poolUrl: "postgresql://p@h:6432/d", sessionUrl: SERVER });

    assert.deepEqual(config.broker.connection, { kind: "pgbouncerTransaction", sessionDatabaseUrl: { kind: "value", value: SERVER } });
  });
});

describe("the SQL literal", () => {
  test("a quote is doubled", () => {
    assert.equal(sqlText("it's"), "'it''s'");
  });
});

describe("the marker files", () => {
  /** Run `check` with the marker directory set to a new directory, then remove it. */
  function withMarkerDir(check: (directory: string) => void): void {
    const directory = mkdtempSync(join(tmpdir(), "horsies-ts-e2e-unit-"));
    const before = process.env[MARKER_DIR_ENV];

    process.env[MARKER_DIR_ENV] = directory;

    try {
      check(directory);
    } finally {
      if (before === undefined) {
        delete process.env[MARKER_DIR_ENV];
      } else {
        process.env[MARKER_DIR_ENV] = before;
      }

      rmSync(directory, { recursive: true, force: true });
    }
  }

  test("an exclusive create makes the marker once, then finds it", () => {
    withMarkerDir(() => {
      assert.deepEqual(createOnce("a", "x"), { ok: true, value: "created" });
      assert.deepEqual(createOnce("a", "y"), { ok: true, value: "existed" });
    });
  });

  test("the attempts of a token count from 1, up to the bound", () => {
    withMarkerDir(() => {
      assert.deepEqual(nextAttempt("t"), { ok: true, value: 1 });
      assert.deepEqual(nextAttempt("t"), { ok: true, value: 2 });

      for (let attempt = 3; attempt <= MAX_ATTEMPTS; attempt += 1) {
        assert.deepEqual(nextAttempt("t"), { ok: true, value: attempt });
      }

      assert.deepEqual(nextAttempt("t"), { ok: false, error: { kind: "tooManyAttempts" } });
    });
  });

  test("an append adds one line; a file in a missing folder is an io error", () => {
    withMarkerDir((directory) => {
      assert.deepEqual(appendLine("lines", "one"), { ok: true, value: undefined });

      process.env[MARKER_DIR_ENV] = join(directory, "missing");

      const failed = appendLine("lines", "two");

      assert.equal(failed.ok ? "appended" : failed.error.kind, "io");
    });
  });

  test("no marker directory is directoryNotSet", () => {
    withMarkerDir(() => {
      delete process.env[MARKER_DIR_ENV];

      assert.deepEqual(createOnce("a", "x"), { ok: false, error: { kind: "directoryNotSet" } });
    });
  });
});
