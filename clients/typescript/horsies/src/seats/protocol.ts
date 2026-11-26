// The message protocol between a seat isolate of a `SHARED_THREAD` child and the main thread of the child.
//
// The main thread starts each isolate from the app entry with `workerData = { horsiesSeat: { seatIndex, protocol,
// cancelCells } }`: the seat of the isolate, the protocol version, and the cancel cells of the child (one `Int32Array`
// over a `SharedArrayBuffer`, one cell per seat). Each message is one member of a closed union, told apart by `kind`,
// and carries the protocol version.
//
// Main thread to seat:
// - `run`: run the body of `taskName` for the run `runKey` with the stored input text, the task id, the attempt (the
//   fencing token's value, from 1) and the facts for the seat body observer. The answer is `result`.
// - `cancel`: the cancel signal of the run `runKey` fired with `cause`. No answer: the isolate aborts the run's signal.
// - `opReply`: the answer to the operation `requestId`.
//
// Seat to main thread:
// - `ready`: the isolate ran every provider of the app; it takes runs.
// - `bootFailed`: the provider `hookName` threw or rejected; the child ends with 173.
// - `result`: the stored outcome of the run `runKey`.
// - `op`: one core operation of an app area (a send, a task read) for the main thread to run. The answer is `opReply`.
//
// A message of another kind or of another protocol version, or a field that is missing or of another type, is a defect
// of the package: the parse throws `ClientDefect`. On the main thread the defect rule then ends the child; in an
// isolate the throw ends the isolate. The values inside a field (an outcome's error parts, the facts of a run, an
// operation and its reply) come from the code of this package in the same process; the parse checks their
// discriminant and passes them on.

import type { SeatOperationMap } from "../app/core-access.ts";
import { isBigint, isNumber, isObjectLike, isString } from "../codec/values.ts";
import type { BodyFactsRecord, CancelCauseName } from "../native/child/addon.ts";
import type { NativeThrown } from "../native/call/envelope.ts";
import type { TaskErrorParts } from "../native/sends/addon.ts";
import { SEAT_MARKER_KEY } from "../app/suppression.ts";
import { ClientDefect, type CoreErrorCode } from "../thrown-errors.ts";
import { unreachable } from "../wire/unreachable.ts";

/** The version of the protocol. An isolate runs the code of the same entry, so both sides hold the same version. */
export const SEAT_PROTOCOL_VERSION = 1;

/** The protocol version of this package. */
export type SeatProtocolVersion = typeof SEAT_PROTOCOL_VERSION;

/** The facts that the main thread gives a seat isolate at its start. `seatIndex` is from 0. */
export type SeatMarker = {
  readonly seatIndex: number;
  readonly protocol: SeatProtocolVersion;
  readonly cancelCells: Int32Array;
};

/** An area of the app whose core operations a seat isolate sends to the main thread. */
export type SeatArea = keyof SeatOperationMap;

/** Each area by itself: the record names each member of `SeatArea` once, so a new area is a type change here. */
const SEAT_AREA_SET: { readonly [Area in SeatArea]: Area } = { sends: "sends", workflows: "workflows" };

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
      readonly facts: BodyFactsRecord;
    }
  | {
      readonly kind: "cancel";
      readonly protocol: SeatProtocolVersion;
      readonly runKey: bigint;
      readonly cause: CancelCauseName;
    }
  | {
      readonly kind: "opReply";
      readonly protocol: SeatProtocolVersion;
      readonly requestId: number;
      readonly answer: SeatAnswer;
    };

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
    };

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
        facts: factsOf(objectField(message, "facts")),
      };
    case "cancel":
      return { kind, protocol, runKey: runKeyField(message), cause: causeField(message) };
    case "opReply":
      return { kind, protocol, requestId: requestIdField(message), answer: answerOf(objectField(message, "answer")) };
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

  return {
    kind: "seat",
    marker: Object.freeze({ seatIndex, protocol: protocolOf(marker, "a seat marker"), cancelCells: cells }),
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
  const value: unknown = Reflect.get(message, "runKey");

  if (!isBigint(value)) {
    throw protocolDefect("the field runKey of a seat message is not a bigint");
  }

  return value;
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
    case "CORE_START_FAILED":
    case "TOO_MANY_PENDING_CALLS":
    case "FORKED_AFTER_CORE_START":
      // SAFETY: the main thread built the refusal from the facts of the `CoreError` of this code.
      return refusal as RelayedRefusal;
    default:
      throw protocolDefect(`the refusal of a seat operation has the code ${String(code)}, which is no CoreError code`);
  }
}
