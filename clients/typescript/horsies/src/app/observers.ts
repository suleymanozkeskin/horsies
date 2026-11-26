// The observer of sends: the seam where the tracing part wraps a send in a span and gives its trace carrier. The
// default observer does nothing and gives no carrier.

import type { TraceArgument } from "../native/values/records.ts";

/** The facts of a send that an observer may record. */
export type SendSpanFacts = {
  readonly taskName: string;
  readonly queueName: string;
  readonly taskId: string;
};

/** Wraps each send; `run` makes the enqueue with the carrier that the observer gives. */
export interface SendObserver {
  observeSend<T>(facts: SendSpanFacts, run: (trace: TraceArgument) => Promise<T>): Promise<T>;
  /** The carrier of the span that is active in the caller, or no carrier. */
  activeCarrier(): TraceArgument;
}

const NO_CARRIER: TraceArgument = Object.freeze({ kind: "noCarrier" });

/** The observer of an app with no tracing: no span, no carrier. */
export const NO_OP_OBSERVER: SendObserver = Object.freeze({
  observeSend: <T>(_facts: SendSpanFacts, run: (trace: TraceArgument) => Promise<T>): Promise<T> => run(NO_CARRIER),
  activeCarrier: (): TraceArgument => NO_CARRIER,
});
