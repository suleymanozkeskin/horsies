// The kind of a zod schema where a contract enters the app (`Horsies.create`, a workflow's params): the self-check
// once per process, then the adapter.

import type { Kind } from "../wire/kinds.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { compileMember, compileValue, type ZodMemberKind } from "./adapter.ts";
import type { ZodSchema } from "./definition.ts";
import type { ZodSchemaRefused } from "./rules.ts";
import { type SelfCheckFailure, type SelfCheckOutcome, selfCheckOnce } from "./self-check.ts";

/** Why the adapter gives no kind for a zod schema. */
export type ZodAdapterRefused =
  /** The schema has forms without a stored form: every refusal, in walk order. */
  | { readonly kind: "SchemaRefused"; readonly refusals: readonly ZodSchemaRefused[] }
  /**
   * The installed zod reads its known schemas to other kinds than this adapter expects (a renamed definition field):
   * every zod schema is refused until horsies reads the new field.
   */
  | { readonly kind: "SelfCheckFailed"; readonly failures: readonly SelfCheckFailure[] };

/**
 * The kind and the presence of a zod schema as an object member or a task input parameter.
 *
 * Fails with `SelfCheckFailed` when the self-check of this process failed (it runs at the first call), else with
 * `SchemaRefused`. Reads each default value twice and calls the getter of each `z.lazy()` once. Does no I/O.
 */
export function zodMemberKind(schema: ZodSchema): Result<ZodMemberKind, ZodAdapterRefused> {
  return afterSelfCheck(selfCheckOnce(), () => compileMember(schema));
}

/**
 * The kind of a zod schema as a whole value (a task output): `.optional()`, `.default()` and `.prefault()` are refused
 * at its top. Fails and reads as `zodMemberKind`.
 */
export function zodValueKind(schema: ZodSchema): Result<Kind, ZodAdapterRefused> {
  return afterSelfCheck(selfCheckOnce(), () => compileValue(schema));
}

/**
 * The compiled kind when the self-check passed: `SelfCheckFailed` with its failures, else the compile's outcome, its
 * refusals as `SchemaRefused`. The compile does not run after a failed self-check. Pure apart from the compile.
 */
export function afterSelfCheck<T>(
  selfCheck: SelfCheckOutcome,
  compile: () => Result<T, readonly ZodSchemaRefused[]>,
): Result<T, ZodAdapterRefused> {
  if (!selfCheck.ok) {
    return err(Object.freeze({ kind: "SelfCheckFailed", failures: selfCheck.error }));
  }

  const compiled = compile();

  return compiled.ok ? ok(compiled.value) : err(Object.freeze({ kind: "SchemaRefused", refusals: compiled.error }));
}
