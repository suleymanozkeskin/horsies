// The payload guardrail of the config: the input sizes at which a send warns and at which it is refused (the core's
// `PayloadPolicy`, `core/src/core/config/payload.rs`).

import { unreachable } from "../wire/unreachable.ts";
import {
  accepted,
  type Checked,
  checkedInteger,
  checkedObject,
  type ConfigJson,
  memberPath,
  optionalEntry,
  U64_FIELD,
} from "./checked.ts";

/** A size bound in bytes of the input text, or no bound. */
export type PayloadBound = { readonly kind: "bytes"; readonly bytes: number } | { readonly kind: "noBound" };

/** The payload guardrail. Each field left out has the core's default. */
export interface PayloadPolicyInput {
  /** A send above this size logs a warning, once per task name. */
  readonly warnBytes?: PayloadBound;
  /** A send above this size is refused with `PAYLOAD_TOO_LARGE`. */
  readonly rejectBytes?: PayloadBound;
}

/** The core's `payload` object of `input`. Pure. */
export function payloadJson(input: PayloadPolicyInput, path: string): Checked<ConfigJson> {
  return checkedObject([
    optionalEntry("warn_bytes", input.warnBytes, (bound) => boundJson(bound, memberPath(path, "warnBytes.bytes"))),
    optionalEntry("reject_bytes", input.rejectBytes, (bound) =>
      boundJson(bound, memberPath(path, "rejectBytes.bytes")),
    ),
  ]);
}

/** The core's form of a bound: the number, or `null` for no bound. Pure. */
function boundJson(bound: PayloadBound, path: string): Checked<ConfigJson> {
  switch (bound.kind) {
    case "bytes":
      return checkedInteger(bound.bytes, U64_FIELD, path);
    case "noBound":
      return accepted(null);
    default:
      return unreachable(bound, "boundJson");
  }
}
