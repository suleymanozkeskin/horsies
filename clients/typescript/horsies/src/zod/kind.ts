// The wire type of a zod schema where a contract enters the app (`Horsies.create`, a workflow's params). The contract
// compile of the main entry point imports these two functions statically. They import no zod module, so the main
// entry point loads and bundles with or without zod. Each gives a wire type: the contract compile reads the kind with
// `kindOf` and `presenceOf`.
//
// Steps, each a closed refusal: the zod release that built the schema (`version.ts`), the adapter, then `checkKind`
// on the compiled kind. Each wire type that these functions give has passed all three. The self-check of the adapter
// runs in the zod test lanes, once per supported zod release (`tests/unit/zod/self-check-cases.ts`): it needs zod's
// builders, which the main entry point does not import.

import { checkKind, checkMemberKind, type KindRefused } from "../wire/check.ts";
import type { RequiredWire, ValueWire } from "../wire/infer.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { memberWireOf, wireOf } from "../wire/wire-type.ts";
import { compileMember, compileValue, type ZodMemberKind } from "./adapter.ts";
import type { ZodSchema } from "./internals.ts";
import type { ZodSchemaRefused } from "./rules.ts";
import { type StandardSchemaMarked, type ZodReleaseRefused, zodSchemaOf } from "./version.ts";

/** Why the adapter gives no kind for a zod schema. */
export type ZodAdapterRefused =
  | ZodReleaseRefused
  /** The schema has forms without a stored form: every refusal, in walk order. */
  | { readonly kind: "SchemaRefused"; readonly refusals: readonly ZodSchemaRefused[] }
  /** The compiled kind breaks a rule of the wire types (`checkKind`), with the rules of `h.*`. */
  | { readonly kind: "KindRefused"; readonly refusals: readonly KindRefused[] };

/**
 * The wire type of a zod schema as an object member or a task input parameter: its kind and its presence. The caller
 * checks the member's name (`checkParameters`).
 *
 * Fails with `Zod3Schema` or `ZodVersionNotSupported` (the release), `SchemaRefused` (the adapter) or `KindRefused`
 * (`checkKind`). A schema of each zod copy passes (the ES module or the CommonJS build, a bundled copy). Reads each
 * default value once (`checkKind`) and calls the getter of each `z.lazy()` once. Does no I/O.
 */
export function zodMemberWire(schema: StandardSchemaMarked): Result<ValueWire, ZodAdapterRefused> {
  const member = kindAfterChecks(schema, compileMember, (compiled: ZodMemberKind) =>
    checkMemberKind(compiled.kind, compiled.presence),
  );

  return member.ok ? ok(memberWireOf(member.value.kind, member.value.presence)) : member;
}

/**
 * The wire type of a zod schema as a whole value (a task output): `.optional()`, `.default()` and `.prefault()` are
 * refused at its top. Fails and reads as `zodMemberWire`.
 */
export function zodValueWire(schema: StandardSchemaMarked): Result<RequiredWire, ZodAdapterRefused> {
  const value = kindAfterChecks(schema, compileValue, checkKind);

  return value.ok ? ok(wireOf<unknown>(value.value)) : value;
}

/**
 * The steps of `zodMemberWire` and `zodValueWire`, with the compile and the kind check as parameters. A step runs only
 * after the steps before it passed. Pure apart from the steps.
 */
function kindAfterChecks<T>(
  schema: StandardSchemaMarked,
  compile: (schema: ZodSchema) => Result<T, readonly ZodSchemaRefused[]>,
  check: (compiled: T) => readonly KindRefused[],
): Result<T, ZodAdapterRefused> {
  const zod4 = zodSchemaOf(schema);

  if (!zod4.ok) {
    return zod4;
  }

  const compiled = compile(zod4.value);

  if (!compiled.ok) {
    return err(Object.freeze({ kind: "SchemaRefused", refusals: compiled.error }));
  }

  const refusals = check(compiled.value);

  return refusals.length === 0 ? ok(compiled.value) : err(Object.freeze({ kind: "KindRefused", refusals }));
}
