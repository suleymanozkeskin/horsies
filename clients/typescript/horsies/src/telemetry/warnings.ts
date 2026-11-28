// The warnings of the telemetry that are not a span not made (`not-made.ts` reports those): a point of the producer
// metrics, the carrier of a rerun, the flush of the tracer provider. A throw of the user's SDK never changes a send or
// a run, and is written once per process and kind, so a broken SDK does not fill the log.

/** Where the telemetry met a throw of the user's SDK. */
export type TelemetryWarningKind = "producerMetric" | "rerunCarrier" | "flush";

/** The text of each kind, in front of the cause. */
const WARNING_TEXT: Readonly<Record<TelemetryWarningKind, string>> = Object.freeze({
  producerMetric: "a point of the producer metrics was not recorded",
  rerunCarrier: "the carrier of a rerun did not inject; the rerun goes on with no carrier",
  flush: "the flush of the tracer provider failed",
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
