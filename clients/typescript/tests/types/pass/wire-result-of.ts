// Compile-pass cases: the value of an h.resultOf parameter, and reading a task error.

import { h, type LibraryTaskCode, type TaskOutcome } from "../../../horsies/src/index.ts";
import type { SentOf } from "../../../horsies/src/wire/infer.ts";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

function expectType<T extends true>(): T | undefined {
  return undefined;
}

const receipt = h.object({ receiptId: h.string() });
const upstream = h.resultOf(receipt);

// A body gets the outcome wrapper; a sender can never give it.
expectType<Equal<h.Infer<typeof upstream>, TaskOutcome<{ receiptId: string }>>>();

expectType<Equal<SentOf<typeof upstream>, never>>();

/** The outcome narrows on its kind, and the code on its family. */
export function describeOutcome(outcome: h.Infer<typeof upstream>): string {
  switch (outcome.kind) {
    case "succeeded":
      return outcome.value.receiptId;
    case "failed":
      switch (outcome.error.code.kind) {
        case "library": {
          const code: LibraryTaskCode = outcome.error.code.code;

          return code;
        }

        case "user":
          return outcome.error.code.code;
      }
  }
}
