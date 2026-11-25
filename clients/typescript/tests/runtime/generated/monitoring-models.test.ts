import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import {
  decodeBreakdown,
  decodeBreakdownCounts,
  decodeBudgetLoadInfo,
  decodeClassReportInfo,
  decodeDownSlotInfo,
  decodeErrorFacet,
  decodeFacetValue,
  decodeFacets,
  decodeGroupRow,
  decodeLeafTaskInfo,
  decodeLivenessReport,
  decodePendingStreamInfo,
  decodeScheduleStateInfo,
  decodeStatusCount,
  decodeTaskAttemptInfo,
  decodeTaskDetail,
  decodeTaskListPage,
  decodeTaskSummary,
  decodeWorkerClassLoadInfo,
  decodeWorkerHistoryPoint,
  decodeWorkerPingInfo,
  decodeWorkerStateInfo,
  decodeWorkflowEdge,
  decodeWorkflowNodeInfo,
  decodeWorkflowRunDetail,
  decodeWorkflowRunSummary,
  decodeWorkflowTaskDetail,
} from "../../../horsies/src/_catalog/monitoring_models.ts";

import type {
  MonitoringDecodeError,
  TaskAttemptInfoPhases,
  TaskAttemptInfoPhasesCapturedClaims,
  TaskAttemptInfoPhasesCapturedFinalizing,
  TaskAttemptInfoPhasesCapturedRecordedBy,
  TaskAttemptInfoPhasesCapturedRecordedByWorkerPlacement,
  WorkflowNodeInfoTiming,
  WorkflowNodeInfoTimingExactEndpoint,
  WorkflowNodeInfoTimingExactSegments,
  WorkflowNodeInfoTimingExactSegmentsPhase,
  WorkflowNodeInfoTimingExactSegmentsPhaseClaimedClaim,
  WorkflowNodeInfoTimingExactSegmentsSource,
  WorkflowNodeInfoTimingInconsistentConflict,
  WorkflowRunDetailTiming,
  WorkflowRunDetailTimingComposedCoverage,
  WorkflowRunDetailTimingComposedCoverageInconsistentConflicts,
  WorkflowRunDetailTimingComposedPauses,
} from "../../../horsies/src/_catalog/monitoring_models.ts";
import type { DecodeResult, WireJson } from "../../../horsies/src/_catalog/wire_json.ts";

/** The contract of the core's monitoring models, which the decoders are rendered from. */
const CONTRACT_URL = new URL("../../../../../core/src/monitoring/model_contract.json", import.meta.url);

/** The core's JSON of each variant of the attempt phases and of the node and run timings. */
const TIMING_EXAMPLES_URL = new URL(
  "../../../../../core/src/monitoring/timing_examples.json",
  import.meta.url,
);

/** One sample of each monitoring model, as the core writes it. */
const SAMPLES_URL = new URL("monitoring-model-samples.json", import.meta.url);

/** An integer text. The reviver reads it from the source, so an integer past 2^53 stays exact. */
const INTEGER_TEXT = /^-?\d+$/;

/** A leaf value as `JSON.parse` gives it to a reviver. */
type RevivedLeaf = null | boolean | number | string;

/** A container as `JSON.parse` gives it to a reviver: its members are `WireJson` already. */
type RevivedContainer = ReadonlyArray<WireJson> | { readonly [key: string]: WireJson };

/** The third argument of a reviver: the source text of a leaf value. */
interface ReviverContext {
  readonly source?: string;
}

/** A decoder of one monitoring model. */
type ModelDecoder = (value: WireJson) => DecodeResult<object, MonitoringDecodeError>;

/** Whether a reviver call is for a leaf: `JSON.parse` gives the source text of leaves only. */
function isLeaf(value: RevivedLeaf | RevivedContainer, source: string | undefined): value is RevivedLeaf {
  return source !== undefined;
}

/** Whether a revived container is a list. */
function isRevivedList(value: RevivedContainer): value is ReadonlyArray<WireJson> {
  return Array.isArray(value);
}

/** One `JSON.parse` value as `WireJson`: an integer text becomes a `bigint`, an object a `Map`. */
function reviveWire(
  _key: string,
  value: RevivedLeaf | RevivedContainer,
  context?: ReviverContext,
): WireJson {
  const source = context?.source;

  if (!isLeaf(value, source)) {
    return isRevivedList(value) ? value : new Map(Object.entries(value));
  }

  if (source !== undefined && INTEGER_TEXT.test(source)) {
    return BigInt(source);
  }

  return value;
}

/** The `WireJson` tree of a JSON text, as the package's lossless reader gives it. */
function readWireJson(text: string): WireJson {
  const tree: WireJson = JSON.parse(text, reviveWire);

  return tree;
}

/** The `WireJson` tree of a JSON file. */
function readWireFile(url: URL): WireJson {
  return readWireJson(readFileSync(url, "utf8"));
}

/** Whether a `WireJson` value is an object: the reader gives each JSON object as a `Map`. */
function isWireObject(value: WireJson): value is ReadonlyMap<string, WireJson> {
  return value !== null && value.constructor === Map;
}

/** Whether a `WireJson` value is a list. */
function isWireList(value: WireJson): value is ReadonlyArray<WireJson> {
  return Array.isArray(value);
}

/** The member `key` of an object value. */
function member(value: WireJson, key: string): WireJson {
  assert.ok(isWireObject(value), `the value that holds ${key} is an object`);

  const found = value.get(key);

  assert.ok(found !== undefined, `the object holds ${key}`);

  return found;
}

/** The items of a list value. */
function itemsOf(value: WireJson): ReadonlyArray<WireJson> {
  assert.ok(isWireList(value), "the value is a list");

  return value;
}

/** A copy of an object value with the member `key` set to `replacement`. */
function withMember(value: WireJson, key: string, replacement: WireJson): WireJson {
  assert.ok(isWireObject(value), `the value that takes ${key} is an object`);

  return new Map(value).set(key, replacement);
}

/** A copy of an object value without the member `key`. */
function withoutMember(value: WireJson, key: string): WireJson {
  assert.ok(isWireObject(value), `the value that loses ${key} is an object`);

  const copy = new Map(value);

  assert.ok(copy.delete(key), `the object holds ${key}`);

  return copy;
}

/** The sample of the model `name`. */
function sample(name: string): WireJson {
  return member(readWireFile(SAMPLES_URL), name);
}

/** The decoded value of `result`. */
function decoded<T>(result: DecodeResult<T, MonitoringDecodeError>): T {
  if (!result.ok) {
    assert.fail(`the value decodes, not ${result.error.kind} at ${result.error.path}`);
  }

  return result.value;
}

/** The error of a refused `result`. */
function refusal<T>(result: DecodeResult<T, MonitoringDecodeError>): MonitoringDecodeError {
  if (result.ok) {
    assert.fail("the value is refused");
  }

  return result.error;
}

/** The exact text of an instant: every fraction digit that it holds. */
function at(instant: Temporal.Instant): string {
  return instant.toString();
}

function claimsText(claims: TaskAttemptInfoPhasesCapturedClaims): string {
  switch (claims.kind) {
    case "counted":
      return `counted(${claims.count}, ${at(claims.firstClaimedAt)})`;
    case "notCounted":
      return "notCounted";
    default: {
      const unreachable: never = claims;

      return unreachable;
    }
  }
}

function finalizingText(finalizing: TaskAttemptInfoPhasesCapturedFinalizing): string {
  switch (finalizing.kind) {
    case "marked":
      return `marked(${at(finalizing.finalizingAt)})`;
    case "notMarked":
      return "notMarked";
    default: {
      const unreachable: never = finalizing;

      return unreachable;
    }
  }
}

function placementText(placement: TaskAttemptInfoPhasesCapturedRecordedByWorkerPlacement): string {
  switch (placement.kind) {
    case "placed":
      return `placed(${at(placement.placedAt)})`;
    case "notObserved":
      return "notObserved";
    default: {
      const unreachable: never = placement;

      return unreachable;
    }
  }
}

function recordedByText(recordedBy: TaskAttemptInfoPhasesCapturedRecordedBy): string {
  switch (recordedBy.kind) {
    case "worker":
      return `worker(${placementText(recordedBy.placement)})`;
    case "recovery":
      return `recovery(${at(recordedBy.lastSeenAt)})`;
    case "cancel":
      return "cancel";
    default: {
      const unreachable: never = recordedBy;

      return unreachable;
    }
  }
}

/** The attempt phases as one line of text, for a compact comparison. */
function phasesText(phases: TaskAttemptInfoPhases): string {
  switch (phases.kind) {
    case "notCaptured":
      return "notCaptured";
    case "captured":
      return [
        `captured(${at(phases.enqueuedAt)}, ${at(phases.claimedAt)})`,
        claimsText(phases.claims),
        finalizingText(phases.finalizing),
        recordedByText(phases.recordedBy),
      ].join(" ");
    default: {
      const unreachable: never = phases;

      return unreachable;
    }
  }
}

function claimText(claim: WorkflowNodeInfoTimingExactSegmentsPhaseClaimedClaim): string {
  switch (claim.kind) {
    case "earlier":
      return `earlier(${claim.count})`;
    case "recorded":
      return `recorded(${placementText(claim.placement)})`;
    case "notRecorded":
      return "notRecorded";
    default: {
      const unreachable: never = claim;

      return unreachable;
    }
  }
}

function segmentPhaseText(phase: WorkflowNodeInfoTimingExactSegmentsPhase): string {
  switch (phase.kind) {
    case "claimed":
      return `claimed(${claimText(phase.claim)})`;
    case "unknown":
      return `unknown(${phase.reason})`;
    case "advancing":
    case "enqueued":
    case "finalizing":
    case "pending":
    case "ready":
    case "retryWait":
    case "running":
      return phase.kind;
    default: {
      const unreachable: never = phase;

      return unreachable;
    }
  }
}

function sourceText(source: WorkflowNodeInfoTimingExactSegmentsSource): string {
  switch (source.kind) {
    case "node":
      return "node";
    case "task":
      return `task(${source.generation}, ${source.taskId})`;
    case "attempt":
      return `attempt(${source.generation}, ${source.taskId}, ${source.attempt})`;
    default: {
      const unreachable: never = source;

      return unreachable;
    }
  }
}

function segmentText(segment: WorkflowNodeInfoTimingExactSegments): string {
  const end = segment.end.kind === "ended" ? `ended(${at(segment.end.at)})` : "open";

  return `${segmentPhaseText(segment.phase)} ${at(segment.startedAt)} ${end} ${sourceText(segment.source)}`;
}

function endpointText(endpoint: WorkflowNodeInfoTimingExactEndpoint): string {
  switch (endpoint.kind) {
    case "terminal":
      return `terminal(${endpoint.status}, ${at(endpoint.at)})`;
    case "notTerminal":
      return "notTerminal";
    default: {
      const unreachable: never = endpoint;

      return unreachable;
    }
  }
}

function conflictText(conflict: WorkflowNodeInfoTimingInconsistentConflict): string {
  switch (conflict.kind) {
    case "clockOrder":
      return `clockOrder(${conflict.earlier} ${at(conflict.earlierAt)}, ${conflict.later} ${at(conflict.laterAt)})`;
    case "missingRecord":
      return `missingRecord(${conflict.record})`;
    case "unexpectedEvent":
      return `unexpectedEvent(${conflict.eventId}, ${conflict.event})`;
    default: {
      const unreachable: never = conflict;

      return unreachable;
    }
  }
}

/** The node timing as lines of text, for a compact comparison. */
function nodeTimingLines(timing: WorkflowNodeInfoTiming): ReadonlyArray<string> {
  switch (timing.kind) {
    case "exact":
      return ["exact", ...timing.segments.map(segmentText), endpointText(timing.endpoint)];
    case "partial":
      return [
        `partial(${timing.reasons.join(", ")})`,
        ...timing.segments.map(segmentText),
        endpointText(timing.endpoint),
      ];
    case "unavailable":
      return [`unavailable(${timing.reason})`];
    case "inconsistent":
      return [`inconsistent(${conflictText(timing.conflict)})`];
    default: {
      const unreachable: never = timing;

      return unreachable;
    }
  }
}

function runConflictText(conflict: WorkflowRunDetailTimingComposedCoverageInconsistentConflicts): string {
  switch (conflict.kind) {
    case "node":
      return `node(${conflict.taskIndex})`;
    case "pause":
      return `pause(${conflictText(conflict.conflict)})`;
    default: {
      const unreachable: never = conflict;

      return unreachable;
    }
  }
}

function coverageText(coverage: WorkflowRunDetailTimingComposedCoverage): string {
  switch (coverage.kind) {
    case "exact":
      return "exact";
    case "partial":
      return `partial(${coverage.reasons.join(", ")})`;
    case "inconsistent":
      return `inconsistent(${coverage.conflicts.map(runConflictText).join(", ")})`;
    default: {
      const unreachable: never = coverage;

      return unreachable;
    }
  }
}

function pauseText(pause: WorkflowRunDetailTimingComposedPauses): string {
  switch (pause.end.kind) {
    case "resumed":
      return `${at(pause.startedAt)} resumed(${at(pause.end.at)})`;
    case "runEnded":
      return `${at(pause.startedAt)} runEnded(${at(pause.end.at)})`;
    case "open":
      return `${at(pause.startedAt)} open`;
    default: {
      const unreachable: never = pause.end;

      return unreachable;
    }
  }
}

/** The run timing as lines of text, for a compact comparison. */
function runTimingLines(timing: WorkflowRunDetailTiming): ReadonlyArray<string> {
  switch (timing.kind) {
    case "unavailable":
      return [`unavailable(${timing.reason}, ${at(timing.asOf)}, ${at(timing.capturedSince)})`];
    case "composed":
      return [
        `composed(${at(timing.asOf)}, ${at(timing.capturedSince)}, ${coverageText(timing.coverage)})`,
        `range(${at(timing.rangeStartedAt)}, ${at(timing.rangeEndedAt)}, open: ${timing.openSegments})`,
        ...timing.pauses.map(pauseText),
      ];
    default: {
      const unreachable: never = timing;

      return unreachable;
    }
  }
}

/** The examples of one kind in the core's timing examples file. */
function timingExamples(kind: string): ReadonlyArray<WireJson> {
  return itemsOf(member(readWireFile(TIMING_EXAMPLES_URL), kind));
}

/** The decoder of each model of the contract, by model name. */
const DECODERS: ReadonlyMap<string, ModelDecoder> = new Map<string, ModelDecoder>([
  ["Breakdown", decodeBreakdown],
  ["BreakdownCounts", decodeBreakdownCounts],
  ["BudgetLoadInfo", decodeBudgetLoadInfo],
  ["ClassReportInfo", decodeClassReportInfo],
  ["DownSlotInfo", decodeDownSlotInfo],
  ["ErrorFacet", decodeErrorFacet],
  ["FacetValue", decodeFacetValue],
  ["Facets", decodeFacets],
  ["GroupRow", decodeGroupRow],
  ["LeafTaskInfo", decodeLeafTaskInfo],
  ["LivenessReport", decodeLivenessReport],
  ["PendingStreamInfo", decodePendingStreamInfo],
  ["ScheduleStateInfo", decodeScheduleStateInfo],
  ["StatusCount", decodeStatusCount],
  ["TaskAttemptInfo", decodeTaskAttemptInfo],
  ["TaskDetail", decodeTaskDetail],
  ["TaskListPage", decodeTaskListPage],
  ["TaskSummary", decodeTaskSummary],
  ["WorkerClassLoadInfo", decodeWorkerClassLoadInfo],
  ["WorkerHistoryPoint", decodeWorkerHistoryPoint],
  ["WorkerPingInfo", decodeWorkerPingInfo],
  ["WorkerStateInfo", decodeWorkerStateInfo],
  ["WorkflowEdge", decodeWorkflowEdge],
  ["WorkflowNodeInfo", decodeWorkflowNodeInfo],
  ["WorkflowRunDetail", decodeWorkflowRunDetail],
  ["WorkflowRunSummary", decodeWorkflowRunSummary],
  ["WorkflowTaskDetail", decodeWorkflowTaskDetail],
]);

void describe("monitoring model decoders", () => {
  void test("every model of the contract has a decoder, and its sample decodes to a frozen value", () => {
    const contract = readWireFile(CONTRACT_URL);

    assert.ok(isWireObject(contract), "the contract is an object of models");
    assert.deepEqual([...DECODERS.keys()], [...contract.keys()]);

    for (const [name, decode] of DECODERS) {
      assert.ok(Object.isFrozen(decoded(decode(sample(name)))), name);
    }
  });

  void test("field names are camelCase, tags are camelCase and enum values keep the core's text", () => {
    const worker = decoded(decodeWorkerStateInfo(sample("WorkerStateInfo")));

    assert.equal(worker.workerId, "worker-a");
    assert.equal(worker.profile.kind, "notRecorded");
    assert.equal(worker.slotHealth, "slots_down");
    assert.equal(worker.clusterWideCap, null);
    assert.equal(worker.memoryUsageMb, 512.25);
    assert.equal(worker.uptimeS, 21_550);
    assert.deepEqual(worker.queues, ["emails", "reports"]);
    assert.deepEqual([...worker.queueMaxConcurrency], [
      ["emails", 4],
      ["reports", 2],
    ]);
    assert.equal(worker.downSlots[0]?.failedBoots, 4);
    assert.equal(worker.downSlots[0]?.cause, "BOOT_FAILED");
  });

  void test("an integer is a number and a float keeps its fraction", () => {
    const stream = decoded(decodeClassReportInfo(sample("ClassReportInfo"))).streams[0];

    assert.ok(stream !== undefined, "the report has a stream");
    assert.equal(stream.wait.kind === "waiting" ? stream.wait.waitS : null, 30.310417);
    assert.equal(stream.coverage.kind === "served" ? stream.coverage.capacity : null, 4);
    assert.equal(decoded(decodePendingStreamInfo(sample("PendingStreamInfo"))).execution, "ISOLATED");
    assert.equal(decoded(decodeStatusCount(sample("StatusCount"))).count, 8);
  });

  void test("each union variant of the sample models decodes with its tag", () => {
    const report = decoded(decodeClassReportInfo(sample("ClassReportInfo")));
    const breakdown = decoded(decodeBreakdown(sample("Breakdown")));

    assert.deepEqual(
      report.streams.map((stream) => `${stream.wait.kind} ${stream.coverage.kind}`),
      ["waiting served", "noneDue noCapacity", "noneDue unserved"],
    );
    assert.deepEqual(
      breakdown.groups.map((row) => (row.group.kind === "value" ? row.group.value : row.group.kind)),
      ["emails", "unclaimed"],
    );
  });

  void test("a nullable field is null or its value", () => {
    const leaf = decoded(decodeLeafTaskInfo(sample("LeafTaskInfo")));
    const detail = decoded(decodeTaskDetail(sample("TaskDetail")));
    const node = decoded(decodeWorkflowTaskDetail(sample("WorkflowTaskDetail")));

    assert.equal(leaf.failedAt, null);
    assert.equal(leaf.queueS, 2.310417);
    assert.equal(leaf.execution, "SHARED_ASYNC");
    assert.equal(detail.leaf.execution, null);
    assert.equal(detail.workflowTaskIndex, null);
    assert.equal(node.leaf, null);
    assert.deepEqual(node.attempts, []);
  });

  void test("a map keeps its keys in order, and an empty list is a list", () => {
    const facets = decoded(decodeFacets(sample("Facets")));

    assert.deepEqual([...facets.errorCategoryTotals], [
      ["timeout", 7],
      ["user", 2],
      ["library", 0],
    ]);
    assert.deepEqual(facets.workers, []);
    assert.equal(facets.errorCodes[0]?.category, "timeout");
  });

  void test("lists and nested records are frozen", () => {
    const page = decoded(decodeTaskListPage(sample("TaskListPage")));

    assert.ok(Object.isFrozen(page.rows));
    assert.ok(Object.isFrozen(page.rows[0]));
    assert.equal(page.rows[0]?.id, "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b");
  });
});

void describe("date-time values", () => {
  void test("a date-time keeps its microseconds", () => {
    const leaf = decoded(decodeLeafTaskInfo(sample("LeafTaskInfo")));

    assert.ok(leaf.startedAt !== null, "the task started");
    assert.equal(at(leaf.startedAt), "2026-11-17T13:59:16.810417Z");
    assert.equal(leaf.enqueuedAt?.epochNanoseconds, 1_794_923_954_500_000_000n);
    assert.equal(
      at(decoded(decodeScheduleStateInfo(sample("ScheduleStateInfo"))).updatedAt),
      "2026-11-17T00:00:00.000001Z",
    );
    assert.equal(at(decoded(decodeDownSlotInfo(sample("DownSlotInfo"))).downSince), "2026-11-17T13:50:00.00025Z");
  });

  void test("a date-time of whole seconds and a leap day decode", () => {
    for (const text of ["2026-11-17T13:59:14Z", "2024-02-29T00:00:00Z", "2026-11-17T13:59:14.000001Z"]) {
      const schedule = decoded(decodeScheduleStateInfo(withMember(sample("ScheduleStateInfo"), "updated_at", text)));

      assert.ok(schedule.updatedAt.equals(Temporal.Instant.from(text)), text);
    }
  });

  void test("a text that is not the core's date-time form is dateTimeMalformed", () => {
    const refused = [
      "2026-11-17T13:59:14.500Z",
      "2026-11-17T13:59:14.5000000Z",
      "2026-11-17T13:59:14+00:00",
      "2026-11-17 13:59:14Z",
      "2026-11-17T13:59:14z",
      "2026-02-30T00:00:00Z",
      "2026-02-29T00:00:00Z",
      "2026-13-01T00:00:00Z",
      "2026-11-17T24:00:00Z",
      "2026-11-17T13:60:00Z",
      "2026-11-17T13:59:60Z",
      "+012026-11-17T13:59:14Z",
    ];

    for (const text of refused) {
      assert.deepEqual(
        refusal(decodeScheduleStateInfo(withMember(sample("ScheduleStateInfo"), "updated_at", text))),
        { kind: "dateTimeMalformed", model: "ScheduleStateInfo", path: "$.updated_at", text },
      );
    }
  });
});

void describe("timing examples of the core", () => {
  void test("every attempt phase example decodes in a task attempt", () => {
    const lines = timingExamples("attempt_phases").map((example) =>
      phasesText(decoded(decodeTaskAttemptInfo(withMember(sample("TaskAttemptInfo"), "phases", example))).phases),
    );

    assert.deepEqual(lines, [
      "notCaptured",
      "captured(2026-11-22T12:00:00Z, 2026-11-22T12:00:02Z) counted(2, 2026-11-22T12:00:01Z) marked(2026-11-22T12:00:08Z) worker(placed(2026-11-22T12:00:03Z))",
      "captured(2026-11-22T12:00:00Z, 2026-11-22T12:00:02Z) notCounted notMarked recovery(2026-11-22T12:00:07Z)",
      "captured(2026-11-22T12:00:00Z, 2026-11-22T12:00:02Z) counted(1, 2026-11-22T12:00:02Z) notMarked worker(notObserved)",
      "captured(2026-11-22T12:00:00Z, 2026-11-22T12:00:02Z) counted(1, 2026-11-22T12:00:02Z) marked(2026-11-22T12:00:07Z) cancel",
    ]);
  });

  void test("every node timing example decodes in a workflow node", () => {
    const task = "00000000-0000-0000-0000-00000000000a";

    const lines = timingExamples("node_timings").map((example) =>
      nodeTimingLines(decoded(decodeWorkflowNodeInfo(withMember(sample("WorkflowNodeInfo"), "timing", example))).timing),
    );

    assert.deepEqual(lines, [
      [
        "exact",
        "pending 2026-11-22T12:00:00Z ended(2026-11-22T12:00:01Z) node",
        "ready 2026-11-22T12:00:01Z ended(2026-11-22T12:00:02Z) node",
        `enqueued 2026-11-22T12:00:02Z ended(2026-11-22T12:00:03Z) attempt(1, ${task}, 1)`,
        `claimed(earlier(1)) 2026-11-22T12:00:03Z ended(2026-11-22T12:00:04Z) attempt(1, ${task}, 1)`,
        `claimed(recorded(placed(2026-11-22T12:00:05Z))) 2026-11-22T12:00:04Z ended(2026-11-22T12:00:06Z) attempt(1, ${task}, 1)`,
        `running 2026-11-22T12:00:06Z ended(2026-11-22T12:00:07Z) attempt(1, ${task}, 1)`,
        `finalizing 2026-11-22T12:00:07Z ended(2026-11-22T12:00:08Z) attempt(1, ${task}, 1)`,
        `retryWait 2026-11-22T12:00:08Z ended(2026-11-22T12:00:09Z) attempt(1, ${task}, 2)`,
        `claimed(notRecorded) 2026-11-22T12:00:09Z ended(2026-11-22T12:00:10Z) attempt(1, ${task}, 2)`,
        "advancing 2026-11-22T12:00:10Z ended(2026-11-22T12:00:11Z) node",
        "terminal(COMPLETED, 2026-11-22T12:00:12Z)",
      ],
      [
        "partial(HISTORY_PRUNED, WORKER_LOST)",
        `unknown(HISTORY_PRUNED) 2026-11-22T12:00:00Z ended(2026-11-22T12:00:05Z) task(1, ${task})`,
        "unknown(WORKER_LOST) 2026-11-22T12:00:05Z open attempt(2, 00000000-0000-0000-0000-00000000000b, 1)",
        "notTerminal",
      ],
      ["unavailable(RUN_BEFORE_CAPTURE)"],
      ["inconsistent(clockOrder(CLAIM 2026-11-22T12:00:05Z, START 2026-11-22T12:00:04Z))"],
      ["inconsistent(missingRecord(COMPLETION_TIME))"],
      ["inconsistent(unexpectedEvent(7, NODE_ENQUEUED))"],
    ]);
  });

  void test("every run timing example decodes in a workflow run", () => {
    const lines = timingExamples("run_timings").map((example) =>
      runTimingLines(decoded(decodeWorkflowRunDetail(withMember(sample("WorkflowRunDetail"), "timing", example))).timing),
    );

    assert.deepEqual(lines, [
      ["unavailable(TIMING_BOUND_EXCEEDED, 2026-11-22T12:01:40Z, 2026-11-22T12:00:00Z)"],
      [
        "composed(2026-11-22T12:01:40Z, 2026-11-22T12:00:00Z, exact)",
        "range(2026-11-22T12:00:00Z, 2026-11-22T12:01:40Z, open: true)",
        "2026-11-22T12:00:01Z resumed(2026-11-22T12:00:02Z)",
        "2026-11-22T12:00:03Z runEnded(2026-11-22T12:00:04Z)",
        "2026-11-22T12:00:05Z open",
      ],
      [
        "composed(2026-11-22T12:01:40Z, 2026-11-22T12:00:00Z, partial(CLAIMS_NOT_COUNTED))",
        "range(2026-11-22T12:00:00Z, 2026-11-22T12:00:50Z, open: false)",
      ],
      [
        "composed(2026-11-22T12:01:40Z, 2026-11-22T12:00:00Z, inconsistent(node(2), pause(unexpectedEvent(4, WORKFLOW_RESUMED))))",
        "range(2026-11-22T12:00:00Z, 2026-11-22T12:00:50Z, open: false)",
      ],
    ]);
  });
});

void describe("refusals", () => {
  void test("a missing field is fieldMissing", () => {
    assert.deepEqual(refusal(decodeStatusCount(withoutMember(sample("StatusCount"), "count"))), {
      kind: "fieldMissing",
      model: "StatusCount",
      path: "$.count",
    });
  });

  void test("a field that the model does not have is fieldUnknown, in a record and in a variant", () => {
    const attempt = sample("TaskAttemptInfo");

    assert.deepEqual(refusal(decodeStatusCount(withMember(sample("StatusCount"), "share", 1n))), {
      kind: "fieldUnknown",
      model: "StatusCount",
      path: "$.share",
    });
    assert.deepEqual(
      refusal(
        decodeTaskAttemptInfo(
          withMember(attempt, "phases", withMember(member(attempt, "phases"), "enqueued_at", null)),
        ),
      ),
      { kind: "fieldUnknown", model: "TaskAttemptInfo", path: "$.phases.enqueued_at" },
    );
  });

  void test("a value of another JSON kind is kindMismatch with the expected kind", () => {
    const cases: ReadonlyArray<readonly [WireJson, string, string]> = [
      [withMember(sample("StatusCount"), "count", "8"), "$.count", "integer"],
      [withMember(sample("StatusCount"), "count", 8), "$.count", "integer"],
      [withMember(sample("StatusCount"), "status", null), "$.status", "string"],
      [withMember(sample("StatusCount"), "status", new Map()), "$.status", "string"],
    ];

    for (const [input, path, expected] of cases) {
      assert.deepEqual(refusal(decodeStatusCount(input)), {
        kind: "kindMismatch",
        model: "StatusCount",
        path,
        expected,
      });
    }

    assert.deepEqual(refusal(decodeStatusCount([])), {
      kind: "kindMismatch",
      model: "StatusCount",
      path: "$",
      expected: "object",
    });
    assert.deepEqual(refusal(decodeWorkerPingInfo(withMember(sample("WorkerPingInfo"), "round_trip_ms", 3n))), {
      kind: "kindMismatch",
      model: "WorkerPingInfo",
      path: "$.round_trip_ms",
      expected: "number",
    });
    assert.deepEqual(refusal(decodeWorkerStateInfo(withMember(sample("WorkerStateInfo"), "stale", 0n))), {
      kind: "kindMismatch",
      model: "WorkerStateInfo",
      path: "$.stale",
      expected: "boolean",
    });
    assert.deepEqual(refusal(decodeFacets(withMember(sample("Facets"), "queues", new Map()))), {
      kind: "kindMismatch",
      model: "Facets",
      path: "$.queues",
      expected: "list",
    });
    assert.deepEqual(refusal(decodeFacets(withMember(sample("Facets"), "error_category_totals", []))), {
      kind: "kindMismatch",
      model: "Facets",
      path: "$.error_category_totals",
      expected: "object",
    });
  });

  void test("the path names list items and map entries", () => {
    const facets = sample("Facets");
    const queues = itemsOf(member(facets, "queues"));
    const firstQueue = queues[0] ?? null;

    assert.deepEqual(refusal(decodeFacets(withMember(facets, "queues", [withoutMember(firstQueue, "count")]))), {
      kind: "fieldMissing",
      model: "Facets",
      path: "$.queues[0].count",
    });
    assert.deepEqual(
      refusal(decodeFacets(withMember(facets, "error_category_totals", new Map([["time out", "7"]])))),
      { kind: "kindMismatch", model: "Facets", path: '$.error_category_totals["time out"]', expected: "integer" },
    );
  });

  void test("an integer outside the safe range of a number is integerUnsafe", () => {
    const input = readWireJson('{"status": "PENDING", "count": 9007199254740992}');

    assert.deepEqual(refusal(decodeStatusCount(input)), {
      kind: "integerUnsafe",
      model: "StatusCount",
      path: "$.count",
      value: 9_007_199_254_740_992n,
    });
    assert.deepEqual(refusal(decodeStatusCount(withMember(input, "count", -9_007_199_254_740_992n))), {
      kind: "integerUnsafe",
      model: "StatusCount",
      path: "$.count",
      value: -9_007_199_254_740_992n,
    });
    assert.equal(decoded(decodeStatusCount(withMember(input, "count", 9_007_199_254_740_991n))).count, 9_007_199_254_740_991);
  });

  void test("a UUID text that is not the core's form is uuidMalformed", () => {
    for (const text of ["0190A1B2-C3D4-7E5F-8A9B-0C1D2E3F4A5B", "0190a1b2c3d47e5f8a9b0c1d2e3f4a5b", "{0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b}"]) {
      assert.deepEqual(refusal(decodeTaskSummary(withMember(sample("TaskSummary"), "id", text))), {
        kind: "uuidMalformed",
        model: "TaskSummary",
        path: "$.id",
        text,
      });
    }
  });

  void test("a text that is not a value of the enum is enumValueUnknown; another kind is kindMismatch", () => {
    assert.deepEqual(refusal(decodeBudgetLoadInfo(withMember(sample("BudgetLoadInfo"), "budget", "THREADED"))), {
      kind: "enumValueUnknown",
      model: "BudgetLoadInfo",
      path: "$.budget",
      text: "THREADED",
    });
    assert.deepEqual(refusal(decodeBudgetLoadInfo(withMember(sample("BudgetLoadInfo"), "serves", ["ISOLATED", 1n]))), {
      kind: "kindMismatch",
      model: "BudgetLoadInfo",
      path: "$.serves[1]",
      expected: "string",
    });
  });

  void test("a tag that names no variant is tagUnknown; a missing or other tag is fieldMissing or kindMismatch", () => {
    const worker = sample("WorkerStateInfo");
    const profile = member(worker, "profile");

    assert.deepEqual(refusal(decodeWorkerStateInfo(withMember(worker, "profile", withMember(profile, "kind", "anonymous")))), {
      kind: "tagUnknown",
      model: "WorkerStateInfo",
      path: "$.profile.kind",
      tag: "anonymous",
    });
    assert.deepEqual(refusal(decodeWorkerStateInfo(withMember(worker, "profile", withoutMember(profile, "kind")))), {
      kind: "fieldMissing",
      model: "WorkerStateInfo",
      path: "$.profile.kind",
    });
    assert.deepEqual(refusal(decodeWorkerStateInfo(withMember(worker, "profile", withMember(profile, "kind", 1n)))), {
      kind: "kindMismatch",
      model: "WorkerStateInfo",
      path: "$.profile.kind",
      expected: "string",
    });
  });

  void test("a refusal deep in a union names the full path, and the error is frozen", () => {
    const detail = sample("TaskDetail");
    const attempt = itemsOf(member(detail, "attempts"))[0] ?? null;
    const phases = member(attempt, "phases");
    const recordedBy = member(phases, "recorded_by");
    const placement = withMember(member(recordedBy, "placement"), "placed_at", "2026-11-22");

    const input = withMember(detail, "attempts", [
      withMember(attempt, "phases", withMember(phases, "recorded_by", withMember(recordedBy, "placement", placement))),
    ]);

    const error = refusal(decodeTaskDetail(input));

    assert.deepEqual(error, {
      kind: "dateTimeMalformed",
      model: "TaskDetail",
      path: "$.attempts[0].phases.recorded_by.placement.placed_at",
      text: "2026-11-22",
    });
    assert.ok(Object.isFrozen(error));
  });
});
