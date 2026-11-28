// The message protocol between a seat isolate of a `SHARED_THREAD` child and the main thread of the child.
//
// The main thread starts each isolate from the app entry with `workerData = { horsiesSeat: { seatIndex, protocol,
// cancelCells, relayBounds } }`: the seat of the isolate, the protocol version, the cancel cells of the child (one
// `Int32Array` over a `SharedArrayBuffer`, one cell per seat), and the bounds of the isolate's relay (from the core's
// constants, which the main thread reads from the addon). Each message is one member of a closed union, told apart by
// `kind`, and carries the protocol version.
//
// Main thread to seat:
// - `run`: run the body of `taskName` for the run `runKey` with the stored input text, the task id, the attempt (the
//   fencing token's value, from 1) and the facts for the seat body observer. The answer is `result`.
// - `cancel`: the cancel signal of the run `runKey` fired with `cause`. No answer: the isolate aborts the run's signal.
// - `bodyEnded`: the verdict of the run `runKey` (how its body ended, from the run's cancel cause and its outcome)
//   and the main thread's time of its settle, for the seat body observer. No answer.
// - `opReply`: the answer to the operation `requestId`.
// - `flush`: the child stops; the isolate flushes its seat body observer (the tracer provider of its SDK). The answer
//   is `flushed`.
//
// Seat to main thread:
// - `ready`: the isolate ran every provider of the app; it takes runs.
// - `bootFailed`: the provider `hookName` threw or rejected; the child ends with 173.
// - `result`: the stored outcome of the run `runKey`.
// - `op`: one core operation of an app area (a send, a task read, a facade call) for the main thread to run. The answer
//   is `opReply`.
// - `defect`: a defect of the package in the isolate; the main thread applies the defect rule (the child aborts).
// - `log`: one line that the isolate's process events write (a detached throw, a rejection that no body awaits).
// - `flushed`: the flush of the seat body observer settled.
// - `spanNotMade`: the user's SDK threw at a span of the isolate (a send, a workflow start, or the attempt span of a
//   run); the main thread writes the record of the process (`src/telemetry/not-made.ts`).
//
// A message of another kind or of another protocol version, or a field that is missing or of another type, is a defect
// of the package: the parse throws `ClientDefect`. On the main thread the defect rule then ends the child; in an
// isolate the throw ends the isolate. The values inside a field (an outcome's error parts, the facts of a run, an
// operation and its reply) come from the code of this package in the same process; the parse checks their
// discriminant and passes them on.

import type { SeatOperationMap } from "../app/core-access.ts";
import { isBigint, isNumber, isObjectLike, isString } from "../codec/values.ts";
import type { BodyEnd } from "../child/body-observer.ts";
import type { BodyFactsRecord, CancelCauseName } from "../native/child/addon.ts";
import type { NativeThrown } from "../native/call/envelope.ts";
import type { NotMadeSpan } from "../native/logging/addon.ts";
import type { TaskErrorParts } from "../native/sends/addon.ts";
import { SEAT_MARKER_KEY } from "../app/suppression.ts";
import { ClientDefect, type CoreDefectCode, type CoreErrorCode } from "../thrown-errors.ts";
import { unreachable } from "../wire/unreachable.ts";

/** The version of the protocol. An isolate runs the code of the same entry, so both sides hold the same version. */
export const SEAT_PROTOCOL_VERSION = 1;

/** The protocol version of this package. */
export type SeatProtocolVersion = typeof SEAT_PROTOCOL_VERSION;

/**
 * The bounds of the relay of one isolate: the operations that wait for I/O of the main thread at once (the child's
 * task-send pool), and the operations of each lane that wait in the isolate. Whole numbers from 1.
 */
export type RelayBounds = { readonly opsInFlight: number; readonly opsWaiting: number };

/** The facts that the main thread gives a seat isolate at its start. `seatIndex` is from 0. */
export type SeatMarker = {
  readonly seatIndex: number;
  readonly protocol: SeatProtocolVersion;
  readonly cancelCells: Int32Array;
  readonly relayBounds: RelayBounds;
};

/** An area of the app whose core operations a seat isolate sends to the main thread. */
export type SeatArea = keyof SeatOperationMap;

/** Each area by itself: the record names each member of `SeatArea` once, so a new area is a type change here. */
const SEAT_AREA_SET: { readonly [Area in SeatArea]: Area } = { sends: "sends", facades: "facades", workflows: "workflows" };

/** The areas of the seat operations. */
export const SEAT_AREAS: readonly SeatArea[] = Object.values(SEAT_AREA_SET);

/** One core operation of an area that a seat isolate asks for. */
export type SeatRequest = {
  readonly [Area in SeatArea]: { readonly area: Area; readonly operation: SeatOperationMap[Area]["operation"] };
}[SeatArea];

/** The reply of an area to one of its operations. */
export type SeatReply = {
  readonly [Area in SeatArea]: { readonly area: Area; readonly reply: SeatOperationMap[Area]["reply"] };
}[SeatArea];

/** A refusal of a core call that the main thread gives to the isolate: the process-state refusals (`CoreError`). */
export type RelayedRefusal = Extract<NativeThrown, { readonly code: CoreErrorCode }>;

/** The answer to one operation: the area's reply, or the refusal that the core call threw on the main thread. */
export type SeatAnswer =
  | { readonly kind: "reply"; readonly reply: SeatReply }
  | { readonly kind: "refused"; readonly refusal: RelayedRefusal };

/** The stored outcome of a run, as the isolate gives it: the stored text of the value, or the parts of the error. */
export type SeatOutcome =
  | { readonly kind: "value"; readonly json: string }
  | { readonly kind: "error"; readonly parts: TaskErrorParts };

/** A message of the main thread to a seat isolate. */
export type MainToSeat =
  | {
      readonly kind: "run";
      readonly protocol: SeatProtocolVersion;
      readonly runKey: bigint;
      readonly taskName: string;
      readonly inputJson: string;
      readonly taskId: string;
      readonly attempt: number;
      readonly maxRetries: number;
      readonly facts: BodyFactsRecord;
    }
  | {
      readonly kind: "cancel";
      readonly protocol: SeatProtocolVersion;
      readonly runKey: bigint;
      readonly cause: CancelCauseName;
    }
  | {
      readonly kind: "bodyEnded";
      readonly protocol: SeatProtocolVersion;
      readonly runKey: bigint;
      readonly end: BodyEnd;
      /** The main thread's time of the settle, in nanoseconds since the epoch (a `Temporal.Instant` does not clone). */
      readonly endedAtEpochNanoseconds: bigint;
    }
  | {
      readonly kind: "opReply";
      readonly protocol: SeatProtocolVersion;
      readonly requestId: number;
      readonly answer: SeatAnswer;
    }
  | { readonly kind: "flush"; readonly protocol: SeatProtocolVersion };

/** A message of a seat isolate to the main thread. */
export type SeatToMain =
  | { readonly kind: "ready"; readonly protocol: SeatProtocolVersion; readonly seatIndex: number }
  | {
      readonly kind: "bootFailed";
      readonly protocol: SeatProtocolVersion;
      readonly seatIndex: number;
      readonly hookName: string;
      readonly cause: string;
    }
  | {
      readonly kind: "result";
      readonly protocol: SeatProtocolVersion;
      readonly runKey: bigint;
      readonly outcome: SeatOutcome;
    }
  | {
      readonly kind: "op";
      readonly protocol: SeatProtocolVersion;
      readonly requestId: number;
      readonly request: SeatRequest;
    }
  | {
      readonly kind: "defect";
      readonly protocol: SeatProtocolVersion;
      readonly code: CoreDefectCode;
      readonly name: string;
      readonly message: string;
    }
  | { readonly kind: "log"; readonly protocol: SeatProtocolVersion; readonly line: string }
  | { readonly kind: "flushed"; readonly protocol: SeatProtocolVersion }
  | { readonly kind: "spanNotMade"; readonly protocol: SeatProtocolVersion; readonly span: NotMadeSpan; readonly cause: string };

/** The message of a seat isolate in `message`, as a port gave it. A message outside the protocol: `ClientDefect`. */
export function parseSeatToMain(message: object): SeatToMain {
  const kind: unknown = Reflect.get(message, "kind");
  const protocol = protocolOf(message, "a message of a seat isolate");

  switch (kind) {
    case "ready":
      return { kind, protocol, seatIndex: seatIndexField(message) };
    case "bootFailed":
      return {
        kind,
        protocol,
        seatIndex: seatIndexField(message),
        hookName: textField(message, "hookName"),
        cause: textField(message, "cause"),
      };
    case "result":
      return { kind, protocol, runKey: runKeyField(message), outcome: outcomeOf(objectField(message, "outcome")) };
    case "op":
      return { kind, protocol, requestId: requestIdField(message), request: requestOf(objectField(message, "request")) };
    case "defect":
      return {
        kind,
        protocol,
        code: defectCodeField(message),
        name: textField(message, "name"),
        message: textField(message, "message"),
      };
    case "log":
      return { kind, protocol, line: textField(message, "line") };
    case "flushed":
      return { kind, protocol };
    case "spanNotMade":
      return { kind, protocol, span: notMadeSpanOf(objectField(message, "span")), cause: textField(message, "cause") };
    default:
      throw protocolDefect(`a message of a seat isolate has the unknown kind ${String(kind)}`);
  }
}

/** The message of the main thread in `message`, as a port gave it. A message outside the protocol: `ClientDefect`. */
export function parseMainToSeat(message: object): MainToSeat {
  const kind: unknown = Reflect.get(message, "kind");
  const protocol = protocolOf(message, "a message of the main thread");

  switch (kind) {
    case "run":
      return {
        kind,
        protocol,
        runKey: runKeyField(message),
        taskName: textField(message, "taskName"),
        inputJson: textField(message, "inputJson"),
        taskId: textField(message, "taskId"),
        attempt: wholeNumberField(message, "attempt"),
        maxRetries: wholeNumberField(message, "maxRetries"),
        facts: factsOf(objectField(message, "facts")),
      };
    case "cancel":
      return { kind, protocol, runKey: runKeyField(message), cause: causeField(message) };
    case "bodyEnded":
      return {
        kind,
        protocol,
        runKey: runKeyField(message),
        end: bodyEndOf(objectField(message, "end")),
        endedAtEpochNanoseconds: bigintField(message, "endedAtEpochNanoseconds"),
      };
    case "opReply":
      return { kind, protocol, requestId: requestIdField(message), answer: answerOf(objectField(message, "answer")) };
    case "flush":
      return { kind, protocol };
    default:
      throw protocolDefect(`a message of the main thread has the unknown kind ${String(kind)}`);
  }
}

/** Whether `data` (the `workerData` of a thread) holds a seat marker, and the marker. */
export type SeatMarkerRead = { readonly kind: "seat"; readonly marker: SeatMarker } | { readonly kind: "notSeat" };

/** The seat marker of the `workerData` object `data`. A marker that is not well formed: `ClientDefect`. */
export function seatMarkerOf(data: object): SeatMarkerRead {
  if (!(SEAT_MARKER_KEY in data)) {
    return { kind: "notSeat" };
  }

  const marker = objectField(data, SEAT_MARKER_KEY);
  const cells: unknown = Reflect.get(marker, "cancelCells");

  if (!(cells instanceof Int32Array) || !(cells.buffer instanceof SharedArrayBuffer)) {
    throw protocolDefect("the cancel cells of a seat marker are not an Int32Array over a SharedArrayBuffer");
  }

  const seatIndex = seatIndexField(marker);

  if (seatIndex >= cells.length) {
    throw protocolDefect(`the seat ${String(seatIndex)} of a seat marker has no cancel cell`);
  }

  const bounds = objectField(marker, "relayBounds");
  const relayBounds = Object.freeze({ opsInFlight: boundField(bounds, "opsInFlight"), opsWaiting: boundField(bounds, "opsWaiting") });

  return {
    kind: "seat",
    marker: Object.freeze({ seatIndex, protocol: protocolOf(marker, "a seat marker"), cancelCells: cells, relayBounds }),
  };
}

/** The `workerData` value of `marker`. Pure. */
export function seatWorkerData(marker: SeatMarker): { readonly [SEAT_MARKER_KEY]: SeatMarker } {
  return { [SEAT_MARKER_KEY]: marker };
}

function protocolDefect(rule: string): ClientDefect {
  return new ClientDefect({ subject: "seat protocol", rule });
}

function protocolOf(message: object, what: string): SeatProtocolVersion {
  const protocol: unknown = Reflect.get(message, "protocol");

  if (protocol !== SEAT_PROTOCOL_VERSION) {
    throw protocolDefect(`${what} has the protocol version ${String(protocol)}; this package speaks version 1`);
  }

  return SEAT_PROTOCOL_VERSION;
}

function objectField(message: object, name: string): object {
  const value: unknown = Reflect.get(message, name);

  if (!isObjectLike(value)) {
    throw protocolDefect(`the field ${name} of a seat message is not an object`);
  }

  return value;
}

function textField(message: object, name: string): string {
  const value: unknown = Reflect.get(message, name);

  if (!isString(value)) {
    throw protocolDefect(`the field ${name} of a seat message is not a text`);
  }

  return value;
}

function wholeNumberField(message: object, name: string): number {
  const value: unknown = Reflect.get(message, name);

  if (!isNumber(value) || !Number.isSafeInteger(value) || value < 0) {
    throw protocolDefect(`the field ${name} of a seat message is not a whole number`);
  }

  return value;
}

function runKeyField(message: object): bigint {
  return bigintField(message, "runKey");
}

function bigintField(message: object, name: string): bigint {
  const value: unknown = Reflect.get(message, name);

  if (!isBigint(value)) {
    throw protocolDefect(`the field ${name} of a seat message is not a bigint`);
  }

  return value;
}

/** A bound of a relay: a whole number from 1. */
function boundField(bounds: object, name: string): number {
  const value = wholeNumberField(bounds, name);

  if (value < 1) {
    throw protocolDefect(`the relay bound ${name} of a seat marker is 0`);
  }

  return value;
}

function defectCodeField(message: object): CoreDefectCode {
  const code: unknown = Reflect.get(message, "code");

  switch (code) {
    case "MANIFEST_REFUSED":
    case "PANIC_RULE_FIXED":
    case "CORE_PANIC":
    case "ADDON_OUTSIDE_MAIN_THREAD":
    case "CLIENT_DEFECT":
      return code;
    default:
      throw protocolDefect(`the defect of a seat isolate has the code ${String(code)}, which is no defect code`);
  }
}

function bodyEndOf(end: object): BodyEnd {
  const kind: unknown = Reflect.get(end, "kind");

  switch (kind) {
    case "returned":
    case "timedOut":
    case "cancelled":
      return { kind };
    case "failed":
      return { kind, code: textField(end, "code") };
    default:
      throw protocolDefect(`the verdict of a run has the unknown kind ${String(kind)}`);
  }
}

function notMadeSpanOf(span: object): NotMadeSpan {
  const kind: unknown = Reflect.get(span, "kind");

  switch (kind) {
    case "workflowStart":
      return { kind, workflowId: textField(span, "workflowId") };
    case "send":
    case "attempt":
    case "seatAttempt":
    case "workerEnded":
      return { kind, taskId: textField(span, "taskId") };
    default:
      throw protocolDefect(`the span that a seat isolate did not make has the unknown kind ${String(kind)}`);
  }
}

function requestIdField(message: object): number {
  return wholeNumberField(message, "requestId");
}

function seatIndexField(message: object): number {
  return wholeNumberField(message, "seatIndex");
}

function causeField(message: object): CancelCauseName {
  const value: unknown = Reflect.get(message, "cause");

  switch (value) {
    case "DEADLINE":
    case "OPERATOR":
      return value;
    default:
      throw protocolDefect(`the cancel cause ${String(value)} of a seat message is not DEADLINE or OPERATOR`);
  }
}

function outcomeOf(outcome: object): SeatOutcome {
  const kind: unknown = Reflect.get(outcome, "kind");

  switch (kind) {
    case "value":
      return { kind, json: textField(outcome, "json") };
    case "error":
      // SAFETY: the isolate's body adapter built the parts (`TaskErrorParts`) with the code of this package.
      return { kind, parts: objectField(outcome, "parts") as TaskErrorParts };
    default:
      throw protocolDefect(`the outcome of a run has the unknown kind ${String(kind)}`);
  }
}

function factsOf(facts: object): BodyFactsRecord {
  const kind: unknown = Reflect.get(facts, "kind");

  switch (kind) {
    case "notRecorded":
    case "recorded":
      // SAFETY: the main thread passes on the facts record that the addon posted with the run.
      return facts as BodyFactsRecord;
    default:
      throw protocolDefect(`the body facts of a run have the unknown kind ${String(kind)}`);
  }
}

function areaOf(message: object): SeatArea {
  const area: unknown = Reflect.get(message, "area");
  const known = SEAT_AREAS.find((name) => name === area);

  if (known === undefined) {
    throw protocolDefect(`a seat operation names the unknown area ${String(area)}`);
  }

  return known;
}

function requestOf(request: object): SeatRequest {
  const area = areaOf(request);
  const operation = objectField(request, "operation");

  switch (area) {
    case "sends":
      // SAFETY: the relay of an isolate built the operation with the sends area's own operation type.
      return { area, operation: operation as SeatOperationMap["sends"]["operation"] };
    case "facades":
      // SAFETY: the relay of an isolate built the operation with the facades area's own operation type.
      return { area, operation: operation as SeatOperationMap["facades"]["operation"] };
    case "workflows":
      // SAFETY: the relay of an isolate built the operation with the workflows area's own operation type.
      return { area, operation: operation as SeatOperationMap["workflows"]["operation"] };
    default:
      return unreachable(area, "requestOf");
  }
}

function replyOf(reply: object): SeatReply {
  const area = areaOf(reply);
  const value = objectField(reply, "reply");

  switch (area) {
    case "sends":
      // SAFETY: the main thread built the reply with the sends area's own reply type.
      return { area, reply: value as SeatOperationMap["sends"]["reply"] };
    case "facades":
      // SAFETY: the main thread built the reply with the facades area's own reply type.
      return { area, reply: value as SeatOperationMap["facades"]["reply"] };
    case "workflows":
      // SAFETY: the main thread built the reply with the workflows area's own reply type.
      return { area, reply: value as SeatOperationMap["workflows"]["reply"] };
    default:
      return unreachable(area, "replyOf");
  }
}

function answerOf(answer: object): SeatAnswer {
  const kind: unknown = Reflect.get(answer, "kind");

  switch (kind) {
    case "reply":
      return { kind, reply: replyOf(objectField(answer, "reply")) };

    case "refused":
      return { kind, refusal: refusalOf(objectField(answer, "refusal")) };
    default:
      throw protocolDefect(`the answer to a seat operation has the unknown kind ${String(kind)}`);
  }
}

function refusalOf(refusal: object): RelayedRefusal {
  const code: unknown = Reflect.get(refusal, "code");

  switch (code) {
    case "CORE_STOPPED":
    case "RUNTIME_START_FAILED":
    case "TOO_MANY_PENDING_CALLS":
    case "FORKED_AFTER_CORE_START":
      // SAFETY: the main thread built the refusal from the facts of the `CoreError` of this code.
      return refusal as RelayedRefusal;
    default:
      throw protocolDefect(`the refusal of a seat operation has the code ${String(code)}, which is no CoreError code`);
  }
}
