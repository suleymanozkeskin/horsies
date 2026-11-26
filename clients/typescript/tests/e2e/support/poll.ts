// A poll that gives the value it waited for (Rust `clients/rust/tests/support/src/poll.rs` `poll_until`).

import assert from "node:assert/strict";

import { POLL_INTERVAL_MS, pause } from "./process.ts";

/** What one probe of `pollFor` found: the value, or nothing yet. */
export type Probe<T> = { readonly kind: "found"; readonly value: T } | { readonly kind: "notYet" };

/** A probe that found `value`. Pure. */
export function found<T>(value: T): Probe<T> {
  return { kind: "found", value };
}

/** A probe that found nothing yet. */
export const NOT_YET: Probe<never> = Object.freeze({ kind: "notYet" });

/** Probe every `POLL_INTERVAL_MS` until `probe` finds a value, and give it. Fails the test with `subject` when `limitMs` passes first. */
export async function pollFor<T>(limitMs: number, subject: string, probe: () => Promise<Probe<T>>): Promise<T> {
  const deadline = performance.now() + limitMs;

  for (;;) {
    const probed = await probe();

    switch (probed.kind) {
      case "found":
        return probed.value;
      case "notYet":
        break;
    }

    if (performance.now() >= deadline) {
      assert.fail(`${subject}: not reached within ${String(limitMs)} ms`);
    }

    await pause(POLL_INTERVAL_MS);
  }
}
