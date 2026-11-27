// The observer of sends: the seam where the tracing part wraps a send in a span and gives its trace carrier. The
// default observer does nothing and gives no carrier.

import type { TaskId } from "../contract/ids.ts";
import type { TraceArgument } from "../native/values/records.ts";
import type { SendFailure } from "../send/errors.ts";
import type { Result } from "../wire/result.ts";

/** The facts of a send that an observer may record. */
export type SendSpanFacts = {
  readonly taskName: string;
  readonly queueName: string;
  readonly taskId: string;
};

/** What one send call gives after its database calls: the task id, or the failure. */
export type SendCallResult = Result<TaskId, SendFailure>;

/** Wraps each send; `run` makes the enqueue with the carrier that the observer gives. */
export interface SendObserver {
  /** Run the database calls of one send call inside the observation, and give their result unchanged. */
  observeSend(facts: SendSpanFacts, run: (trace: TraceArgument) => Promise<SendCallResult>): Promise<SendCallResult>;
  /** The carrier of the span that is active in the caller, or no carrier. */
  activeCarrier(): TraceArgument;
}

const NO_CARRIER: TraceArgument = Object.freeze({ kind: "noCarrier" });

/** The observer of an app with no tracing: no span, no carrier. */
export const NO_OP_OBSERVER: SendObserver = Object.freeze({
  observeSend: async (
    _facts: SendSpanFacts,
    run: (trace: TraceArgument) => Promise<SendCallResult>,
  ): Promise<SendCallResult> => run(NO_CARRIER),
  activeCarrier: (): TraceArgument => NO_CARRIER,
});
