// The warnings of the telemetry: a throw of the user's SDK never changes a send or a run, and is written once per
// process and kind, so a broken SDK does not fill the log.

/** Where the telemetry met a throw of the user's SDK. */
export type TelemetryWarningKind =
  | "sendSpanOpen"
  | "sendSpanEnd"
  | "startSpanOpen"
  | "startSpanEnd"
  | "producerMetric"
  | "rerunCarrier"
  | "attemptSpanOpen"
  | "attemptSpanEnd"
  | "flush"
  | "endedSpan";

/** The text of each kind, in front of the cause. */
const WARNING_TEXT: Readonly<Record<TelemetryWarningKind, string>> = Object.freeze({
  sendSpanOpen: "the span of a send did not open; the send goes on with no carrier",
  sendSpanEnd: "the span of a send did not end",
  startSpanOpen: "the span of a workflow start did not open; the start goes on with no carrier",
  startSpanEnd: "the span of a workflow start did not end",
  producerMetric: "a point of the producer metrics was not recorded",
  rerunCarrier: "the carrier of a rerun did not inject; the rerun goes on with no carrier",
  attemptSpanOpen: "the span of an attempt did not open; the body runs with no span",
  attemptSpanEnd: "the span of an attempt did not end",
  flush: "the flush of the tracer provider failed",
  endedSpan: "the span of an attempt that the worker ended was not made",
});

/** The warnings of one process. */
export class TelemetryWarnings {
  readonly #written = new Set<TelemetryWarningKind>();
  readonly #writeLine: (line: string) => void;

  constructor(writeLine: (line: string) => void) {
    this.#writeLine = writeLine;
  }

  /** Write the warning of `kind` with `cause`, if this process wrote none of that kind. Later ones are not written. */
  warnOnce(kind: TelemetryWarningKind, cause: unknown): void {
    if (this.#written.has(kind)) {
      return;
    }

    this.#written.add(kind);

    const text = cause instanceof Error ? `${cause.name}: ${cause.message}` : String(cause);

    this.#writeLine(`horsies telemetry: ${WARNING_TEXT[kind]}: ${text}. Later warnings of this kind are not written.`);
  }
}

/** The warnings of this process: to stderr. */
export const processWarnings: TelemetryWarnings = new TelemetryWarnings((line) => {
  process.stderr.write(`${line}\n`);
});
