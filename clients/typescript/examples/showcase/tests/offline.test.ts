// The tests of the showcase that need no database: the settings rules, the draws, the app and its lists, the check
// command, the arguments of the operator commands, and the private package.

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, test } from "node:test";

import { ACME_PROFILE, appConfig, WORKER_PROFILE } from "../src/app.ts";
import { ALL_TASKS } from "../src/contracts/index.ts";
import { refundPayment } from "../src/contracts/payments.ts";
import { parseCancelArguments } from "../src/operations/cancel.ts";
import { MAX_FAILED_ROWS, parseReportArguments } from "../src/operations/report.ts";
import { DEFAULT_RERUN_REASON, parseRerunArguments } from "../src/operations/rerun.ts";
import { ACME_DATABASE_NAME, resolveDatabaseSettings, type SettingsRefused } from "../src/settings.ts";
import { choice, demandFactor, draw, integer, sample, unit } from "../src/simulate.ts";
import { CARD_DECLINE_RATE, PSP_UNAVAILABLE_RATE } from "../src/tuning.ts";
import { ALL_WORKFLOWS } from "../src/workflows/index.ts";
import { runEntry } from "../scripts/entry-process.ts";
import { testShowcase } from "./showcase.ts";
import { OFFLINE_SETTINGS, OFFLINE_URL } from "./support.ts";

/** The task count of the Rust showcase, which this app ports. */
const TASK_COUNT = 36;

/** The workflow count of the Rust showcase. */
const WORKFLOW_COUNT = 12;

/** The draws of a rate test, and the largest distance of the measured rate from the configured one. */
const RATE_DRAWS = 10_000;
const RATE_TOLERANCE = 0.02;

const TASK_ID = "0190b7a2-1c3e-7f00-8a00-000000000001";

function refusedKind(read: (name: string) => string | undefined): SettingsRefused["kind"] {
  const resolved = resolveDatabaseSettings(read);

  assert.equal(resolved.ok, false);

  return resolved.ok ? assert.fail("resolved") : resolved.error.kind;
}

describe("database settings", () => {
  test("ACME_DATABASE_URL is used as it is", () => {
    const resolved = resolveDatabaseSettings((name) => (name === "ACME_DATABASE_URL" ? "postgresql://u@db:5433/acme_x" : undefined));

    assert.ok(resolved.ok);
    assert.equal(resolved.value.databaseName, "acme_x");
    assert.equal(resolved.value.source, "ACME_DATABASE_URL");
    assert.equal(resolved.value.maintenanceUrl, "postgresql://u@db:5433/postgres");
  });

  test("DATABASE_URL gives the same server with the showcase database name", () => {
    const resolved = resolveDatabaseSettings((name) => (name === "DATABASE_URL" ? "postgres://u@db/other" : undefined));

    assert.ok(resolved.ok);
    assert.equal(resolved.value.databaseName, ACME_DATABASE_NAME);
    assert.equal(resolved.value.url, `postgres://u@db/${ACME_DATABASE_NAME}`);
    assert.equal(resolved.value.source, "DATABASE_URL");
  });

  test("each refusal of the rules", () => {
    const acme = (value: string) => (name: string) => (name === "ACME_DATABASE_URL" ? value : undefined);

    assert.equal(refusedKind(() => undefined), "notSet");
    assert.equal(refusedKind(acme("not a url")), "notUrl");
    assert.equal(refusedKind(acme("mysql://u@db/acme")), "schemeRefused");
    assert.equal(refusedKind(acme("postgresql://u@db")), "noDatabaseName");
    assert.equal(refusedKind(acme("postgresql://u@db/acme-demo")), "databaseNameRefused");
  });
});

describe("draws", () => {
  test("the same parts give the same value; other parts another", () => {
    assert.equal(unit(["order-1", "psp"]), unit(["order-1", "psp"]));
    assert.notEqual(unit(["order-1", "psp"]), unit(["order-2", "psp"]));
    assert.equal(integer(1, 4, ["same"]), integer(1, 4, ["same"]));
  });

  test("a draw falls under its rate at that rate, within the sampling error", () => {
    for (const rate of [PSP_UNAVAILABLE_RATE, CARD_DECLINE_RATE]) {
      let hits = 0;

      for (let index = 0; index < RATE_DRAWS; index += 1) {
        hits += draw(rate, [`order-${String(index)}`]) ? 1 : 0;
      }

      assert.ok(Math.abs(hits / RATE_DRAWS - rate) < RATE_TOLERANCE, `rate ${String(rate)}: ${String(hits)} hits`);
    }
  });

  test("integers, choices and samples stay in their bounds", () => {
    for (let index = 0; index < 1_000; index += 1) {
      const value = integer(2_000, 4_000, [String(index)]);

      assert.ok(value >= 2_000 && value <= 4_000);
    }

    assert.ok(["a", "b", "c"].includes(choice(["a", "b", "c"], ["pick"])));

    const picked = sample(["a", "b", "c", "d"], 3, ["sample"]);

    assert.equal(new Set(picked).size, 3);
    assert.deepEqual(picked, sample(["a", "b", "c", "d"], 3, ["sample"]));
    assert.equal(sample(["a"], 5, ["more than the options"]).length, 1);
  });

  test("the demand factor is positive at every hour", () => {
    const start = Temporal.Instant.from("2026-01-01T00:00:00Z");

    for (let hour = 0; hour < 24; hour += 1) {
      assert.ok(demandFactor(start.add({ hours: hour })) > 0);
    }
  });
});

describe("the app", () => {
  test("it builds with every task and workflow, each name once", () => {
    const showcase = testShowcase(OFFLINE_SETTINGS);

    assert.ok(showcase.app);
    assert.equal(ALL_TASKS.length, TASK_COUNT);
    assert.equal(new Set(ALL_TASKS.map((task) => task.name)).size, TASK_COUNT);
    assert.equal(ALL_WORKFLOWS.length, WORKFLOW_COUNT);
    assert.equal(new Set(ALL_WORKFLOWS.map((workflow) => workflow.name)).size, WORKFLOW_COUNT);
  });

  test("a send that breaks a zod refinement of the refund is VALIDATION_FAILED, with no database", async () => {
    const showcase = testShowcase(OFFLINE_SETTINGS);
    const disputedAt = Temporal.Instant.from("2026-01-15T09:00:00Z");
    const sent = await showcase.app.send(refundPayment, { orderId: "ACME-00001", amountCents: 0, disputedAt });

    assert.equal(sent.ok, false);
    assert.equal(sent.ok ? "sent" : sent.error.code, "VALIDATION_FAILED");
  });

  test("the config names the acme profile and the four queues", () => {
    const config = appConfig(OFFLINE_URL);

    assert.deepEqual(config.workerProfiles?.[WORKER_PROFILE], ACME_PROFILE);
    assert.deepEqual(
      config.queueMode === "custom" ? config.customQueues.map((queue) => queue.name) : [],
      ["payments", "fulfillment", "notifications", "analytics"],
    );
  });

  test("check passes with no database: every contract, implementation and workflow", { timeout: 120_000 }, async () => {
    const run = await runEntry(["check"], { ACME_DATABASE_URL: OFFLINE_URL });

    assert.equal(run.code, 0, run.output);
    assert.match(run.output, /ok: all validations passed/u);
    assert.match(run.output, new RegExp(`${String(TASK_COUNT)} task\\(s\\) registered`, "u"));
  });

  test("the entry with no command prints the usage and exits 2", { timeout: 120_000 }, async () => {
    const run = await runEntry([], { ACME_DATABASE_URL: OFFLINE_URL });

    assert.equal(run.code, 2, run.output);
    assert.match(run.output, /scenarios: seed, steady/u);
  });
});

describe("operator command arguments", () => {
  test("report: the default, a count, and a count out of bounds", () => {
    assert.deepEqual(parseReportArguments([]), { ok: true, value: { failedRows: 10 } });
    assert.deepEqual(parseReportArguments(["--failed", "3"]), { ok: true, value: { failedRows: 3 } });
    assert.equal(parseReportArguments(["--failed", String(MAX_FAILED_ROWS + 1)]).ok, false);
    assert.equal(parseReportArguments(["--unknown"]).ok, false);
  });

  test("rerun: a task id with the default reason; no id; a text that is no task id", () => {
    const parsed = parseRerunArguments([TASK_ID]);

    assert.ok(parsed.ok);
    assert.equal(parsed.value.reason, DEFAULT_RERUN_REASON);
    assert.equal(parseRerunArguments([]).ok, false);
    assert.equal(parseRerunArguments(["ACME-1"]).ok, false);
    assert.equal(parseRerunArguments([TASK_ID, "--reason", ""]).ok, false);
  });

  test("cancel: the default policy, a named policy, and an unknown one", () => {
    const parsed = parseCancelArguments([TASK_ID]);

    assert.ok(parsed.ok);
    assert.equal(parsed.value.policy, "includeRunning");

    const named = parseCancelArguments([TASK_ID, "--policy", "pending-or-claimed-only"]);

    assert.ok(named.ok);
    assert.equal(named.value.policy, "pendingOrClaimedOnly");
    assert.equal(parseCancelArguments([TASK_ID, "--policy", "everything"]).ok, false);
  });
});

test("the showcase package is private: it is never published", () => {
  const manifest: unknown = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));

  assert.deepEqual(Object.entries(Object(manifest)).find(([key]) => key === "private"), ["private", true]);
});
