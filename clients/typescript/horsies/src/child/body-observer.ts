// The body observer: the interface that the body adapter calls around each task body. The default does nothing. The
// attempt-span part of the observability package supplies the OpenTelemetry form, so the attempt span is the active
// context where the body runs; this package never imports the telemetry code.
//
// Order of the calls for one body: `enter` with the facts of the run; `run` with the body call; `thrown` with a value
// that the body threw or rejected with, before the error mapping; `end` with how the body ended. `cancelled` comes
// when the run's cancel signal fires (the core's `cancelRun` post), before or after the other calls: the core then
// decided the end of the run (`TASK_TIMEOUT` at the deadline, `TASK_CANCELLED` at an operator cancel), whatever the
// body does later. The child's exit rule awaits `flush`, within its bound.

import type { AppContext } from "../app/context.ts";
import type { BodyFactsRecord, CancelCauseName } from "../native/child/addon.ts";

/** How a body ended, as the adapter saw it. `code` is the text of the stored error code. */
export type BodyEnd =
  | { readonly kind: "returned" }
  | { readonly kind: "failed"; readonly code: string }
  | { readonly kind: "timedOut" }
  | { readonly kind: "cancelled" };

/** The observation of one body. No method may throw: a throw is a defect of the observer and ends the child. */
export interface BodyObservation {
  /** Run the body call inside the observation (for example inside the attempt span), and give its return. */
  run<T>(call: () => T): T;
  /** The body threw, or its Promise rejected, with `cause`. */
  thrown(cause: unknown): void;
  /** The body ended. */
  end(end: BodyEnd): void;
  /** The run's cancel signal fired with `cause`: the core decided the end of the run, whatever the body does later. */
  cancelled(cause: CancelCauseName): void;
}

/** The observer of the task bodies of a child. */
export interface BodyObserver {
  /** The observation of one body, before the body runs. `facts` is `notRecorded` when the app traces no body. */
  enter(facts: BodyFactsRecord): BodyObservation;
  /** Send what the observer holds; the exit rule waits for it within its bound. */
  flush(): Promise<void>;
}

const NO_OBSERVATION: BodyObservation = Object.freeze({
  run: <T>(call: () => T): T => call(),
  thrown: (): void => undefined,
  end: (): void => undefined,
  cancelled: (): void => undefined,
});

/** The observer that does nothing: `run` calls the body, `flush` resolves at once. */
export const NO_BODY_OBSERVER: BodyObserver = Object.freeze({
  enter: (): BodyObservation => NO_OBSERVATION,
  flush: async (): Promise<void> => undefined,
});

const observers = new WeakMap<AppContext, BodyObserver>();

/** Set the body observer of an app: the extension point of the attempt-span part. A second set replaces the first. */
export function setBodyObserver(context: AppContext, observer: BodyObserver): void {
  observers.set(context, observer);
}

/** The body observer of an app: the one set, or the one that does nothing. */
export function bodyObserverOf(context: AppContext): BodyObserver {
  return observers.get(context) ?? NO_BODY_OBSERVER;
}
