// The report of a span that the package did not make or did not complete: the user's SDK threw at a stage of the span
// of a send, a workflow start, an attempt in a Node child or in a seat isolate, or an attempt that the worker ended.
//
// - One form for every kind, the Python client's: one record at WARNING on `horsies.telemetry`, once per process and
//   kind of span, through the core log of the process (`logSpanNotMade` of the addon), so the thresholds and the sink
//   of `configureLogging` apply. The flags of the once rule are in the addon, one set per process; the handler child's
//   own report of a worker-ended span that it could not hand to JavaScript shares them.
// - On a main thread the addon writes the record. A seat isolate loads no addon: it sends the report to the main
//   thread of its child (`spanNotMade`). Another worker thread has no core: the report is one stderr line.
// - The reports of a realm write the first report of each kind only, so a broken SDK calls the addon or posts to the
//   main thread once per kind.
// - A report never changes a send, a start or a run. A record that the addon refuses because the log forwarder does
//   not start is one stderr line.

import { isMainThread } from "node:worker_threads";

import { envelopeValue, type NativeEnvelope } from "../native/call/envelope.ts";
import { thrownFacts } from "../native/call/thrown-facts.ts";
import { loggingAddon, type NotMadeSpan } from "../native/logging/addon.ts";
import { type SeatChannel, threadChannel } from "../seats/channel.ts";
import { SEAT_PROTOCOL_VERSION } from "../seats/protocol.ts";
import { unreachable } from "../wire/unreachable.ts";

export type { NotMadeSpan } from "../native/logging/addon.ts";

/** Writes the record of one span that was not made, with the text of the cause. */
export type NotMadeWriter = (span: NotMadeSpan, cause: string) => void;

/** The addon call that writes the record. */
export type LogSpanNotMade = (span: NotMadeSpan, cause: string) => NativeEnvelope<undefined>;

/** The reports of one realm: the first report of each kind of span is written; later ones are not. */
export class SpansNotMade {
  readonly #written = new Set<NotMadeSpan["kind"]>();
  readonly #write: NotMadeWriter;

  constructor(write: NotMadeWriter) {
    this.#write = write;
  }

  /** Report that `span` was not made because the user's SDK threw `cause`. */
  report(span: NotMadeSpan, cause: unknown): void {
    const facts = thrownFacts(cause);

    this.reportText(span, `${facts.name}: ${facts.message}`);
  }

  /** Report that `span` was not made, with the text of the cause (the report of a seat isolate). */
  reportText(span: NotMadeSpan, cause: string): void {
    if (this.#written.has(span.kind)) {
      return;
    }

    this.#written.add(span.kind);
    this.#write(span, cause);
  }
}

/**
 * The writer of a main thread: the record of the addon. A forwarder that does not start (`CORE_START_FAILED`) gives
 * one line to `writeLine`; any other thrown code is the class of that code, a defect.
 */
export function addonNotMadeWriter(log: LogSpanNotMade, writeLine: (line: string) => void): NotMadeWriter {
  return (span, cause) => {
    const envelope = log(span, cause);

    switch (envelope.kind) {
      case "value":
        return;
      case "thrown":
        if (envelope.thrown.code === "CORE_START_FAILED") {
          writeLine(`${lineText(span, cause)} The core log did not start: ${envelope.thrown.cause}.`);

          return;
        }

        envelopeValue(envelope);

        return;
      default:
        unreachable(envelope, "addonNotMadeWriter");
    }
  };
}

/** The text of a report on stderr: the text of the record, after the name of the logger. Pure. */
export function lineText(span: NotMadeSpan, cause: string): string {
  const [subject, goesOn] = spanWords(span);

  return `horsies.telemetry: the span of ${subject} was not made or is not complete: ${cause}. ${goesOn}. Later spans of this kind that are not made are not logged.`;
}

/** What the span is of, and what goes on without it. Pure. */
function spanWords(span: NotMadeSpan): readonly [subject: string, goesOn: string] {
  switch (span.kind) {
    case "send":
      return [`the send of task ${span.taskId}`, "The send goes on"];
    case "workflowStart":
      return [`the start of workflow ${span.workflowId}`, "The start goes on"];
    case "attempt":
      return [`the attempt of task ${span.taskId}`, "The run goes on"];
    case "seatAttempt":
      return [`the attempt of task ${span.taskId} in a seat isolate`, "The run goes on"];
    case "workerEnded":
      return [`the attempt of task ${span.taskId} that the worker ended`, "The event still goes to the handlers"];
    default:
      return unreachable(span, "spanWords");
  }
}

/** The writer of a seat isolate: the report goes to the main thread of its child, which writes the record. */
export function channelNotMadeWriter(channel: SeatChannel): NotMadeWriter {
  return (span, cause) => {
    channel.post({ kind: "spanNotMade", protocol: SEAT_PROTOCOL_VERSION, span, cause });
  };
}

/** One stderr line of this process. */
function writeStderrLine(line: string): void {
  process.stderr.write(`${line}\n`);
}

/** The writer of this thread: the addon on a main thread, the channel in a seat isolate, else one stderr line. */
function threadNotMadeWriter(): NotMadeWriter {
  if (isMainThread) {
    return addonNotMadeWriter((span, cause) => loggingAddon().logSpanNotMade(span, cause), writeStderrLine);
  }

  const thread = threadChannel();

  switch (thread.kind) {
    case "seat":
      return channelNotMadeWriter(thread.channel);
    case "notSeat":
      return (span, cause) => {
        writeStderrLine(lineText(span, cause));
      };

    default:
      return unreachable(thread, "threadNotMadeWriter");
  }
}

/** The reports of this realm (a process, or a seat isolate). The writer of the thread is chosen at the first report. */
export const processSpansNotMade: SpansNotMade = new SpansNotMade((span, cause) => {
  threadNotMadeWriter()(span, cause);
});
