// The seat body observer: the interface that a seat isolate calls around each task body, for the attempt span of a
// seat run. The default does nothing. The seat-span part of the observability package supplies the OpenTelemetry form
// at this extension point; this package never imports the telemetry code.
//
// Order of the calls for one body: `enter` with the facts of the run (from the `run` message); `run` with the body
// call; `thrown` with a value that the body threw or rejected with, before the error mapping; `end` with the verdict of
// the run and its time (`endedAt`), from the `bodyEnded` message of the main thread. The main thread gives the verdict
// from the run's cancel cause (a deadline is `timedOut`, an operator cancel `cancelled`) and from its outcome, at its
// settle; `endedAt` is the main thread's time of that settle. A sync body that runs past its deadline ends late in the
// isolate, and its observation ends at its settle.

import type { AppContext } from "../app/context.ts";
import type { BodyEnd, BodyObservation } from "../child/body-observer.ts";
import type { BodyFactsRecord } from "../native/child/addon.ts";

/** The observation of one body in a seat isolate. No method may throw: a throw ends the isolate. */
export interface SeatBodyObservation {
  /** Run the body call inside the observation (for example inside the attempt span), and give its return. */
  run<T>(call: () => T): T;
  /** The body threw, or its Promise rejected, with `cause`. */
  thrown(cause: unknown): void;
  /** The run ended with the verdict `end` at `endedAt` (the main thread's time of its settle). */
  end(end: BodyEnd, endedAt: Temporal.Instant): void;
}

/** The observer of the task bodies of a seat isolate. */
export interface SeatBodyObserver {
  /** The observation of one body, before the body runs. `facts` is `notRecorded` when the app traces no body. */
  enter(facts: BodyFactsRecord): SeatBodyObservation;
}

const NO_SEAT_OBSERVATION: SeatBodyObservation = Object.freeze({
  run: <T>(call: () => T): T => call(),
  thrown: (): void => undefined,
  end: (): void => undefined,
});

/** The observer that does nothing: `run` calls the body. */
export const NO_SEAT_BODY_OBSERVER: SeatBodyObserver = Object.freeze({
  enter: (): SeatBodyObservation => NO_SEAT_OBSERVATION,
});

const observers = new WeakMap<AppContext, SeatBodyObserver>();

/** Set the seat body observer of an app: the extension point of the seat-span part. A second set replaces the first. */
export function setSeatBodyObserver(context: AppContext, observer: SeatBodyObserver): void {
  observers.set(context, observer);
}

/** The seat body observer of an app: the one set, or the one that does nothing. */
export function seatBodyObserverOf(context: AppContext): SeatBodyObserver {
  return observers.get(context) ?? NO_SEAT_BODY_OBSERVER;
}

/**
 * The observation of the body adapter for `observation`: `run` and `thrown` pass on; the adapter's `end` passes
 * nothing, because the observation ends at the main thread's verdict (`bodyEnded`).
 */
export function bodyObservationOf(observation: SeatBodyObservation): BodyObservation {
  return Object.freeze({
    run: <T>(call: () => T): T => observation.run(call),
    thrown: (cause: unknown): void => {
      observation.thrown(cause);
    },
    end: (): void => undefined,
  });
}
