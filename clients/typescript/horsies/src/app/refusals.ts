// The refusal sink: the refusals of registrations after `Horsies.create` (`app.implement`, and later areas). The
// `check` command and the worker start read it; a worker refuses to start while it holds a refusal.

import type { Refusal } from "./refusal.ts";

/** The area of the app whose registration a refusal names. */
export type RefusalArea = "contracts" | "events";

/** A refusal with its area. */
export type AreaRefusal = { readonly area: RefusalArea; readonly refusal: Refusal };

/** The sink of one app. */
export class RefusalSink {
  readonly #recorded: AreaRefusal[] = [];

  /** Keep a refusal of `area`. */
  record(area: RefusalArea, refusal: Refusal): void {
    this.#recorded.push(Object.freeze({ area, refusal }));
  }

  /** Every refusal, in the order of the registrations. */
  recorded(): readonly AreaRefusal[] {
    return Object.freeze([...this.#recorded]);
  }
}
