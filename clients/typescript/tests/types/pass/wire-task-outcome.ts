// Compile-pass cases: the value of an h.taskOutcome parameter, and reading a task error.

import { defineTask, h, type BuiltInTaskCode, type TaskOutcome } from "../../../horsies/src/index.ts";
import type { SentOf } from "../../../horsies/src/wire/infer.ts";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

function expectType<T extends true>(): T | undefined {
  return undefined;
}

const receipt = h.object({ receiptId: h.string() });
const charge = defineTask({ name: "charge", input: {}, output: receipt });
const upstream = h.taskOutcome(charge);

// A body gets the outcome wrapper; a sender can never give it.
expectType<Equal<h.Infer<typeof upstream>, TaskOutcome<{ receiptId: string }>>>();

expectType<Equal<SentOf<typeof upstream>, never>>();

/** The outcome narrows on its kind, and the error on its tag. */
export function describeOutcome(outcome: h.Infer<typeof upstream>): string {
  switch (outcome.kind) {
    case "succeeded":
      return outcome.value.receiptId;
    case "failed":
      switch (outcome.error.kind) {
        case "builtIn": {
          const code: BuiltInTaskCode = outcome.error.code;

          return code;
        }

        case "custom":
          return outcome.error.code;
      }
  }
}
