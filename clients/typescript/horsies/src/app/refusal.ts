// A refusal of `Horsies.create` or of a registration after it: the code, the place and the rule.

import type { HorsiesErrorCode } from "../_catalog/error_codes.ts";

/** The code of a refusal: a code of the core's catalog (`HRS-nnn`). */
export type RefusalCode = HorsiesErrorCode;

/**
 * Why the app refuses a config field, a contract or a registration.
 *
 * - `path`: the config field path (`customQueues[1].name`) or the contract (`tasks[2] charge_payment`).
 * - `message`: the rule that the value breaks, with the place.
 * - `help`: how to fix it; zero or more lines.
 */
export type Refusal = {
  readonly code: RefusalCode;
  readonly path: string;
  readonly message: string;
  readonly help: readonly string[];
};

/** A frozen refusal. Pure. */
export function refusal(code: RefusalCode, path: string, message: string, help: readonly string[]): Refusal {
  return Object.freeze({ code, path, message: `${path}: ${message}`, help: Object.freeze([...help]) });
}
