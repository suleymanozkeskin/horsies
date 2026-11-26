// The kind of a zod schema where a contract enters the app (`Horsies.create`, a workflow's params). The main entry
// loads `horsies/zod` and calls these two functions; it imports nothing else of the adapter.
//
// Steps, each a closed refusal: the zod release that built the schema (`version.ts`), the self-check once per
// process, the adapter, then `checkKind` on the compiled kind. A kind that these functions give has passed all four.

import { checkKind, checkMemberKind, type KindRefused } from "../wire/check.ts";
import type { Kind } from "../wire/kinds.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { compileMember, compileValue, type SelfCheckPassed, type ZodMemberKind } from "./adapter.ts";
import type { ZodSchema } from "./definition.ts";
import type { ZodSchemaRefused } from "./rules.ts";
import { type SelfCheckFailure, type SelfCheckOutcome, selfCheckOnce } from "./self-check.ts";
import { type StandardSchemaMarked, type ZodReleaseRefused, zodSchemaOf } from "./version.ts";

/** Why the adapter gives no kind for a zod schema. */
export type ZodAdapterRefused =
  | ZodReleaseRefused
  /**
   * The installed zod reads its known schemas to other kinds than this adapter expects (a renamed definition field):
   * every zod schema is refused until horsies reads the new field.
   */
  | { readonly kind: "SelfCheckFailed"; readonly failures: readonly SelfCheckFailure[] }
  /** The schema has forms without a stored form: every refusal, in walk order. */
  | { readonly kind: "SchemaRefused"; readonly refusals: readonly ZodSchemaRefused[] }
  /** The compiled kind breaks a rule of the wire types (`checkKind`), with the rules of `h.*`. */
  | { readonly kind: "KindRefused"; readonly refusals: readonly KindRefused[] };

/**
 * The kind and the presence of a zod schema as an object member or a task input parameter. The caller checks the
 * member's name (`checkParameters`).
 *
 * Fails with `Zod3Schema`, `ZodVersionNotSupported` or `OtherZodCopy` (the release), `SelfCheckFailed` (the self-check
 * runs at the first call of the process), `SchemaRefused` (the adapter) or `KindRefused` (`checkKind`). Reads each
 * default value twice and calls the getter of each `z.lazy()` once. Does no I/O.
 */
export function zodMemberKind(schema: StandardSchemaMarked): Result<ZodMemberKind, ZodAdapterRefused> {
  return kindAfterChecks(schema, selfCheckOnce, compileMember, (member) =>
    checkMemberKind(member.kind, member.presence),
  );
}

/**
 * The kind of a zod schema as a whole value (a task output): `.optional()`, `.default()` and `.prefault()` are refused
 * at its top. Fails and reads as `zodMemberKind`.
 */
export function zodValueKind(schema: StandardSchemaMarked): Result<Kind, ZodAdapterRefused> {
  return kindAfterChecks(schema, selfCheckOnce, compileValue, checkKind);
}

/**
 * The steps of `zodMemberKind` and `zodValueKind`, with the self-check, the compile and the kind check as parameters.
 * A step runs only after the steps before it passed. Pure apart from the steps.
 */
export function kindAfterChecks<T>(
  schema: StandardSchemaMarked,
  selfCheck: () => SelfCheckOutcome,
  compile: (passed: SelfCheckPassed, schema: ZodSchema) => Result<T, readonly ZodSchemaRefused[]>,
  check: (compiled: T) => readonly KindRefused[],
): Result<T, ZodAdapterRefused> {
  const zod4 = zodSchemaOf(schema);

  if (!zod4.ok) {
    return zod4;
  }

  const passed = selfCheck();

  if (!passed.ok) {
    return err(Object.freeze({ kind: "SelfCheckFailed", failures: passed.error }));
  }

  const compiled = compile(passed.value, zod4.value);

  if (!compiled.ok) {
    return err(Object.freeze({ kind: "SchemaRefused", refusals: compiled.error }));
  }

  const refusals = check(compiled.value);

  return refusals.length === 0 ? ok(compiled.value) : err(Object.freeze({ kind: "KindRefused", refusals }));
}
