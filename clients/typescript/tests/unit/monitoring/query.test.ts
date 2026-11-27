// The addon arguments of the monitoring queries: the core's defaults, the window in epoch microseconds, and the
// refusals before any call.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import {
  epochMicros,
  taskBreakdownArg,
  taskFacetsArg,
  taskListArg,
  taskStatsArg,
  windowArg,
  workflowRunsArg,
} from "../../../horsies/src/monitoring/query.ts";

const LONE_SURROGATE = "a\uD800";

describe("taskListArg", () => {
  test("a query with no field leaves each count and the sort at the core's default", () => {
    assert.deepEqual(taskListArg({}), {
      ok: true,
      value: {
        window: { since: { kind: "coreDefault" }, until: { kind: "coreDefault" } },
        filters: {
          statuses: [],
          taskNames: [],
          queues: [],
          workers: [],
          errorCodes: [],
          errorCategories: [],
          retried: "any",
        },
        sort: { kind: "coreDefault" },
        offset: { kind: "coreDefault" },
        limit: { kind: "coreDefault" },
      },
    });
  });

  test("the filters, the sort and the page pass as given", () => {
    const arg = taskListArg({
      statuses: ["FAILED"],
      taskNames: ["resize"],
      errorCategories: ["OPERATIONAL"],
      retriedOnly: true,
      sort: { field: "queueSeconds", direction: "ascending" },
      offset: 10,
      limit: 20,
    });

    assert.ok(arg.ok);
    assert.deepEqual(arg.value.filters.statuses, ["FAILED"]);
    assert.equal(arg.value.filters.retried, "retriedOnly");
    assert.deepEqual(arg.value.sort, { kind: "set", field: "queueSeconds", direction: "ascending" });
    assert.deepEqual(arg.value.offset, { kind: "set", value: 10 });
    assert.deepEqual(arg.value.limit, { kind: "set", value: 20 });
  });

  test("an offset or a limit that is not a whole number is invalidInput for the page", () => {
    for (const page of [{ offset: 1.5 }, { limit: Number.NaN }, { limit: 2 ** 53 }]) {
      const arg = taskListArg(page);

      assert.ok(!arg.ok);
      assert.equal(arg.error.kind, "invalidInput");
      assert.equal(arg.error.field, "page");
    }
  });

  test("a filter value that is not well-formed text is invalidInput for its field", () => {
    for (const field of ["taskNames", "queues", "workers", "errorCodes"] as const) {
      const arg = taskListArg({ [field]: ["ok", LONE_SURROGATE] });

      assert.ok(!arg.ok);
      assert.equal(arg.error.field, field);
    }
  });
});

describe("the other queries", () => {
  test("the stats argument has no statuses", () => {
    const arg = taskStatsArg({ queues: ["q"] });

    assert.ok(arg.ok);
    assert.equal("statuses" in arg.value, false);
    assert.deepEqual(arg.value.queues, ["q"]);
  });

  test("the facets argument keeps only the coarse filters", () => {
    assert.deepEqual(taskFacetsArg({ statuses: ["RUNNING"] }), {
      window: { since: { kind: "coreDefault" }, until: { kind: "coreDefault" } },
      statuses: ["RUNNING"],
      errorCategories: [],
      retried: "any",
    });
  });

  test("the breakdown limit is left at the core's default, and a fraction is invalidInput for the limit", () => {
    const arg = taskBreakdownArg({ groupBy: "worker" });

    assert.ok(arg.ok);
    assert.deepEqual(arg.value.limit, { kind: "coreDefault" });

    const refused = taskBreakdownArg({ groupBy: "worker", limit: 0.5 });

    assert.ok(!refused.ok);
    assert.equal(refused.error.field, "limit");
  });

  test("the runs read has no name, no status and the core's default limit when the query names none", () => {
    assert.deepEqual(workflowRunsArg({}), {
      ok: true,
      value: { name: { kind: "anyName" }, status: { kind: "anyStatus" }, limit: { kind: "coreDefault" } },
    });
    assert.deepEqual(workflowRunsArg({ workflowName: "nightly", status: "FAILED", limit: 5 }), {
      ok: true,
      value: {
        name: { kind: "named", name: "nightly" },
        status: { kind: "status", status: "FAILED" },
        limit: { kind: "set", value: 5 },
      },
    });
  });

  test("a workflow name that is not well-formed text is invalidInput", () => {
    const arg = workflowRunsArg({ workflowName: LONE_SURROGATE });

    assert.ok(!arg.ok);
    assert.equal(arg.error.field, "workflowName");
  });
});

describe("the window", () => {
  test("a bound crosses as epoch microseconds", () => {
    const since = Temporal.Instant.from("2026-11-25T12:00:00.123456Z");

    assert.deepEqual(windowArg({ since }), {
      since: { kind: "at", epochMicros: 1_795_608_000_123_456n },
      until: { kind: "coreDefault" },
    });
  });

  test("a part below the microsecond is cut toward the past", () => {
    assert.equal(epochMicros(Temporal.Instant.fromEpochNanoseconds(1_999n)), 1n);
    assert.equal(epochMicros(Temporal.Instant.fromEpochNanoseconds(-1n)), -1n);
    assert.equal(epochMicros(Temporal.Instant.fromEpochNanoseconds(-1_000n)), -1n);
    assert.equal(epochMicros(Temporal.Instant.fromEpochNanoseconds(-1_001n)), -2n);
  });
});
