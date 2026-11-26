// The pure parts that the layers of the scheduler, the health reads, the cancel, the event handlers and the spans add
// to the harness: the span line parser, the poll that gives its value, the instance members and the handler line. No
// database and no process.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { attemptEndedLine } from "../src/handlers.ts";
import { instanceWorkflows } from "../src/instance-members.ts";
import { instanceConfig } from "../src/instances.ts";
import { parseSpanLine } from "../src/span-lines.ts";
import { endedSpansFlow } from "../src/tasks/ended-spans.ts";
import { found, NOT_YET, pollFor } from "../support/poll.ts";

const SERVER = "postgresql://postgres@127.0.0.1:55432/postgres";

/** A span line as the exporter of `span-sdk.ts` writes it, with `changes` over it. */
function lineText(changes: Readonly<Record<string, unknown>> = {}): string {
  return JSON.stringify({
    name: "run e2e_simple",
    traceId: "0af7651916cd43dd8448eb211c80319c",
    spanId: "b7ad6b7169203331",
    parent: { kind: "parent", spanId: "00f067aa0ba902b7" },
    startNanos: "1764163200000000000",
    endNanos: "1764163200500000000",
    status: "UNSET",
    attributes: { "horsies.task.attempt": 1, "horsies.task.execution": "ISOLATED", tags: ["a", null] },
    ...changes,
  });
}

describe("the span lines", () => {
  test("a written line parses, with its parent and its attributes", () => {
    const parsed = parseSpanLine(lineText());

    assert.ok(parsed.ok, parsed.ok ? "" : parsed.error);
    assert.deepEqual(parsed.value.parent, { kind: "parent", spanId: "00f067aa0ba902b7" });
    assert.equal(parsed.value.attributes["horsies.task.attempt"], 1);
  });

  test("a root span parses", () => {
    const parsed = parseSpanLine(lineText({ parent: { kind: "root" } }));

    assert.deepEqual(parsed.ok ? parsed.value.parent : parsed.error, { kind: "root" });
  });

  test("a text that is not JSON is refused", () => {
    const parsed = parseSpanLine("{ not json");

    assert.ok(!parsed.ok && parsed.error.startsWith("not JSON"), JSON.stringify(parsed));
  });

  test("a line with a status outside the codes is refused", () => {
    const parsed = parseSpanLine(lineText({ status: "FAILED" }));

    assert.ok(!parsed.ok && parsed.error.startsWith("not a span line"), JSON.stringify(parsed));
  });
});

describe("the poll that gives its value", () => {
  test("the value of the first probe that finds one", async () => {
    let probes = 0;

    const value = await pollFor(5_000, "the third probe", async () => {
      probes += 1;

      return probes === 3 ? found("third") : NOT_YET;
    });

    assert.deepEqual([value, probes], ["third", 3]);
  });

  test("a probe that finds nothing within the limit fails the test with the subject", async () => {
    await assert.rejects(pollFor(0, "a value that never comes", async () => NOT_YET), {
      name: "AssertionError",
      message: /^a value that never comes: not reached within 0 ms$/,
    });
  });
});

describe("the members of the instances", () => {
  test("only worker_ended_spans has a workflow, and only it has tracing", () => {
    assert.deepEqual(instanceWorkflows("worker_ended_spans"), [endedSpansFlow]);
    assert.deepEqual(instanceWorkflows("default"), []);
    assert.deepEqual(instanceConfig("worker_ended_spans", { kind: "direct", databaseUrl: SERVER }).tracing, { kind: "otel" });
    assert.equal(instanceConfig("handlers", { kind: "direct", databaseUrl: SERVER }).tracing, undefined);
  });

  test("the line of an AttemptEnded event is the task id, the attempt and the end kind", () => {
    assert.equal(attemptEndedLine("7c9e6679-7425-40de-944b-e07fc1f90ae7", 2, "decidedByWorker"), "7c9e6679-7425-40de-944b-e07fc1f90ae7 2 decidedByWorker");
  });
});
