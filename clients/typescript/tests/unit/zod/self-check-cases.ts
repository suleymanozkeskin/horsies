// The self-check of the zod adapter: each rule runs on a known schema, as Python's `verify_shape_rules` does. zod
// gives no stability statement for its definition fields; a renamed field makes a known schema compile to another kind
// or another refusal, and the self-check reports it. A renamed field that the reader needs makes the compile of a
// known schema throw; the self-check reports that throw as a failure of its case.
//
// The self-check builds its known schemas with zod's builders, so it runs where zod is: in the zod test lanes, once
// per supported zod release (`SUPPORTED_ZOD`, `version.ts`), not in the main entry point, which imports no zod. A
// release passes into the supported range only when the self-check passes on it. At run time the adapter refuses a
// schema of a release outside the range (`version.ts`) and every definition part that it does not know
// (`known-fields.ts`).
//
// The cases cover each accepted form and each refusal rule that the installed zod can build, also the forms of later
// zod 4 releases (`z.xor()`, `z.looseRecord()`, `.exactOptional()`) when the installed zod has them.

import { z } from "zod";

import { MAX_VALUE_JSON_DEPTH } from "../../../horsies/src/codec/bounds.ts";
import type { JsonValue } from "../../../horsies/src/codec/json-value.ts";
import { encodeValue } from "../../../horsies/src/codec/writer.ts";
import { h } from "../../../horsies/src/wire/builders.ts";
import type { Kind, Member, MemberPresence, UpstreamOutput } from "../../../horsies/src/wire/kinds.ts";
import { err, ok, type Result } from "../../../horsies/src/wire/result.ts";
import { unreachable } from "../../../horsies/src/wire/unreachable.ts";
import { compileMember, type ZodMemberKind } from "../../../horsies/src/zod/adapter.ts";
import { hz } from "../../../horsies/src/zod/codecs.ts";
import type { ZodSchema } from "../../../horsies/src/zod/internals.ts";
import type { ZodRule } from "../../../horsies/src/zod/rules.ts";

/** What a known schema compiles to: its kind text (`kindText`), or a refusal with one rule. */
export type SelfCheckExpected =
  | { readonly outcome: "kind"; readonly text: string }
  | { readonly outcome: "refused"; readonly rule: ZodRule };

/** A value that the compiled kind of a case writes, or refuses. */
export type SelfCheckProbe = { readonly value: JsonValue; readonly written: "stored" | "refused" };

/** One known schema and what it compiles to. */
export type SelfCheckCase = {
  readonly name: string;
  readonly schema: ZodSchema;
  readonly expected: SelfCheckExpected;
  readonly probes: readonly SelfCheckProbe[];
};

/** A case whose schema compiled to something else: `expected` and `found` are kind texts or rules. */
export type SelfCheckFailure = { readonly name: string; readonly expected: string; readonly found: string };

/** The outcome of a self-check run: passed, or every failure. */
export type SelfCheckOutcome = Result<"passed", readonly SelfCheckFailure[]>;

/**
 * The self-check with the known schemas on the installed zod. Fails with the cases whose schema compiled to another
 * kind or refusal (`SelfCheckFailure`).
 */
export function selfCheck(): SelfCheckOutcome {
  return runSelfCheck(builtCases());
}

/** The known cases, or one failure when building them throws (a zod that lacks a builder of the range). */
function builtCases(): readonly SelfCheckCase[] | SelfCheckFailure {
  try {
    return knownCases();
  } catch (error) {
    return { name: "known schemas", expected: "built", found: thrownText(error) };
  }
}

/** The outcome of compiling each case and writing its probes. Fails with every case that differs. Pure. */
export function runSelfCheck(cases: readonly SelfCheckCase[] | SelfCheckFailure): SelfCheckOutcome {
  const failures = "name" in cases ? [cases] : cases.flatMap(caseFailures);

  return failures.length === 0 ? ok("passed") : err(Object.freeze(failures));
}

/** The failures of one case. A throw of its compile is a failure: a renamed field that the reader needs. */
function caseFailures(known: SelfCheckCase): readonly SelfCheckFailure[] {
  try {
    return compiledCaseFailures(known);
  } catch (error) {
    return [{ name: known.name, expected: expectedText(known.expected), found: thrownText(error) }];
  }
}

/** The text of a throw, for a failure. */
function thrownText(cause: unknown): string {
  return `throws ${cause instanceof Error ? `${cause.name}: ${cause.message}` : String(cause)}`;
}

function compiledCaseFailures(known: SelfCheckCase): readonly SelfCheckFailure[] {
  const compiled = compileMember(known.schema);
  const expected = expectedText(known.expected);
  const found = compiled.ok ? memberText(compiled.value) : compiled.error.map((refused) => refused.rule).join(",");

  if (found !== expected) {
    return [{ name: known.name, expected, found }];
  }

  return compiled.ok ? known.probes.flatMap((probe) => probeFailures(known.name, compiled.value.kind, probe)) : [];
}

function probeFailures(name: string, kind: Kind, probe: SelfCheckProbe): readonly SelfCheckFailure[] {
  const written = encodeValue(kind, probe.value).ok ? "stored" : "refused";

  return written === probe.written
    ? []
    : [{ name, expected: `${JSON.stringify(probe.value)} ${probe.written}`, found: written }];
}

function expectedText(expected: SelfCheckExpected): string {
  switch (expected.outcome) {
    case "kind":
      return expected.text;
    case "refused":
      return expected.rule;
    default:
      return unreachable(expected, "expectedText");
  }
}

/** The text of a member's presence and kind: `?` optional, `=` defaulted, then `kindText`. Pure. */
export function memberText(member: ZodMemberKind): string {
  return `${presenceMark(member.presence)}${kindText(member.kind, 0)}`;
}

function presenceMark(presence: MemberPresence): string {
  switch (presence.mode) {
    case "required":
      return "";
    case "optional":
      return "?";
    case "defaulted":
      return "=";
    default:
      return unreachable(presence, "presenceMark");
  }
}

/**
 * A text of a kind tree: the node name, with its inner kinds in parentheses (`object(a:int,b?:string)`,
 * `tagged.kind(object(...)|object(...))`). A tree deeper than a stored value is cut with `...`. Pure.
 */
export function kindText(kind: Kind, depth: number): string {
  if (depth > MAX_VALUE_JSON_DEPTH) {
    return "...";
  }

  const inner = (next: Kind): string => kindText(next, depth + 1);

  switch (kind.node) {
    case "literal":
      return `literal(${kind.members.map((member) => JSON.stringify(member)).join(",")})`;
    case "union":
      return `union(${kind.members.map(inner).join("|")})`;
    case "taggedUnion":
      return `tagged.${kind.tag}(${kind.variants.map(inner).join("|")})`;
    case "object":
      return `object(${kind.members.map((member) => memberEntryText(member, inner)).join(",")})`;
    case "tuple":
      return `tuple(${kind.items.map(inner).join(",")})`;
    case "array":
      return `array(${inner(kind.item)})`;
    case "record":
      return `record(${inner(kind.value)})`;
    case "nullable":
    case "checked":
      return `${kind.node}(${inner(kind.inner)})`;
    case "taskOutcome":
      return `taskOutcome(${upstreamOutputText(kind.output, inner)})`;
    case "conversion":
      return `conversion(${inner(kind.stored)})`;
    case "null":
    case "boolean":
    case "int":
    case "bigint":
    case "float":
    case "string":
    case "uuid":
    case "decimal":
    case "instant":
    case "offsetDateTime":
    case "plainDateTime":
    case "plainDate":
    case "plainTime":
    case "json":
      return kind.node;
    default:
      return unreachable(kind, "kindText");
  }
}

/** The text of the upstream output of an outcome kind: its kind, or `refused`. */
function upstreamOutputText(output: UpstreamOutput, inner: (kind: Kind) => string): string {
  switch (output.state) {
    case "compiled":
      return inner(output.kind);
    case "refused":
      return "refused";
    default:
      return unreachable(output, "upstreamOutputText");
  }
}

function memberEntryText(member: Member, inner: (kind: Kind) => string): string {
  return `${member.name}:${presenceMark(member.presence)}${inner(member.kind)}`;
}

/** A function default whose value changes at each read. */
function changingDefault(): () => number {
  let reads = 0;

  return () => {
    reads += 1;

    return reads;
  };
}

function kindCase(
  name: string,
  schema: ZodSchema,
  text: string,
  probes: readonly SelfCheckProbe[] = [],
): SelfCheckCase {
  return { name, schema, expected: { outcome: "kind", text }, probes };
}

function refusedCase(name: string, schema: ZodSchema, rule: ZodRule): SelfCheckCase {
  return { name, schema, expected: { outcome: "refused", rule }, probes: [] };
}

/** The known schemas: one per accepted form, and one per refusal rule that a zod schema can reach. */
export function knownCases(): readonly SelfCheckCase[] {
  return [...acceptedCases(), ...containerCases(), ...refusedCases(), ...laterCases()];
}

/** The result of a builder that only later zod 4 releases have, called with `args`; none on an older release. */
function laterSchema(
  holder: object,
  name: string,
  args: readonly (ZodSchema | readonly ZodSchema[])[],
): readonly ZodSchema[] {
  const builder: unknown = Reflect.get(holder, name);

  // SAFETY: a zod builder of a schema returns a zod schema.
  return builder instanceof Function ? [Reflect.apply(builder, holder, args) as ZodSchema] : [];
}

/** The forms of later zod 4 releases, when the installed zod has them. */
function laterCases(): readonly SelfCheckCase[] {
  const xor = laterSchema(z, "xor", [[z.literal("a"), z.string()]]);
  const looseRecord = laterSchema(z, "looseRecord", [z.string(), z.string()]);
  const exactOptional = laterSchema(z.string(), "exactOptional", []);

  return [
    ...xor.map((schema) => refusedCase("xor", schema, "EXCLUSIVE_UNION")),
    ...looseRecord.map((schema) => refusedCase("loose record", schema, "LOOSE_OBJECT")),
    ...exactOptional.map((schema) => kindCase("exact optional", schema, "?string")),
  ];
}

function acceptedCases(): readonly SelfCheckCase[] {
  return [
    kindCase("string", z.string(), "string"),
    kindCase("string format", z.email(), "checked(string)", [{ value: "no", written: "refused" }]),
    kindCase("string check", z.string().max(1), "checked(string)", [{ value: "ab", written: "refused" }]),
    kindCase("int", z.int32(), "checked(int)", [{ value: 2 ** 40, written: "refused" }]),
    kindCase("int check", z.number().int(), "checked(int)"),
    kindCase("uint32", z.uint32(), "checked(int)", [{ value: -1, written: "refused" }]),
    kindCase("float", z.number(), "float"),
    kindCase("float32", z.float32(), "checked(float)"),
    kindCase("bigint", z.bigint(), "bigint"),
    kindCase("int64", z.int64(), "checked(bigint)"),
    kindCase("boolean", z.boolean(), "boolean"),
    kindCase("null", z.null(), "null"),
    kindCase("literal", z.literal(["a", 1]), 'literal("a",1)'),
    kindCase("enum", z.enum(["a", "b"]), 'literal("a","b")'),
    kindCase("template literal", z.templateLiteral(["a", z.int()]), "checked(string)", [
      { value: "b1", written: "refused" },
    ]),
    kindCase("nullable", z.string().nullable(), "nullable(string)"),
    kindCase("nullish", z.string().nullish(), "?nullable(string)"),
    kindCase("prefault", z.string().prefault("x"), "=string"),
    kindCase("nonoptional", z.string().optional().nonoptional(), "string"),
    kindCase("json", z.json(), "json"),
    kindCase("lazy", z.lazy(() => z.string()), "string"),
    kindCase("codec", z.codec(z.string(), z.number(), { decode: Number, encode: String }), "conversion(string)"),
    kindCase("hz codec", hz.instant(), "instant"),
    kindCase("hz wire", hz.wire(h.uuid()), "uuid"),
  ];
}

function containerCases(): readonly SelfCheckCase[] {
  const tagged = z.discriminatedUnion("kind", [
    z.object({ kind: z.literal("a") }),
    z.object({ kind: z.literal("b"), size: z.boolean() }),
  ]);

  return [
    kindCase(
      "object",
      z.object({ a: z.boolean(), b: z.string().optional(), c: z.boolean().default(true) }),
      "object(a:boolean,b:?string,c:=boolean)",
      [{ value: { a: true, d: 1 }, written: "refused" }],
    ),
    kindCase("strict object", z.strictObject({ a: z.null() }), "object(a:null)"),
    kindCase("array", z.array(z.boolean()), "array(boolean)"),
    kindCase("readonly", z.array(z.string()).readonly(), "array(string)"),
    kindCase("tuple", z.tuple([z.string(), z.null()]), "tuple(string,null)"),
    kindCase("record", z.record(z.string(), z.boolean()), "record(boolean)"),
    kindCase("enum record", z.record(z.enum(["a"]), z.boolean()), "checked(record(boolean))", [
      { value: {}, written: "refused" },
    ]),
    kindCase("partial record", z.partialRecord(z.enum(["a"]), z.boolean()), "checked(record(boolean))", [
      { value: {}, written: "stored" },
    ]),
    kindCase("union", z.union([z.string(), z.boolean()]), "union(string|boolean)"),
    kindCase(
      "discriminated union",
      tagged,
      'tagged.kind(object(kind:literal("a"))|object(kind:literal("b"),size:boolean))',
    ),
    kindCase("refine", z.object({}).refine(() => false), "checked(object())", [{ value: {}, written: "refused" }]),
  ];
}

function refusedCases(): readonly SelfCheckCase[] {
  const recursive: z.ZodType = z.lazy(() => z.array(recursive));
  const optionA = z.object({ kind: z.literal("a") });
  const convertedA = z.codec(optionA, optionA, { decode: (value) => value, encode: (value) => value });
  const optionB = z.object({ kind: z.literal("b") });

  return [
    refusedCase("any", z.any(), "UNTYPED"),
    refusedCase("transform", z.string().transform((text) => text.length), "TRANSFORM"),
    refusedCase("coerce", z.coerce.number(), "TRANSFORM"),
    refusedCase("pipe", z.string().pipe(z.string()), "TRANSFORM"),
    refusedCase("trim", z.string().trim(), "TRANSFORM"),
    refusedCase("catch", z.string().catch(""), "CATCH"),
    refusedCase("date", z.date(), "DATE"),
    refusedCase("loose object", z.looseObject({}), "LOOSE_OBJECT"),
    refusedCase("catchall", z.object({}).catchall(z.string()), "LOOSE_OBJECT"),
    refusedCase("map", z.map(z.string(), z.string()), "NOT_DATA"),
    refusedCase("custom", z.custom<string>(), "NOT_DATA"),
    refusedCase("object union", z.union([z.object({}), z.string()]), "UNDISCRIMINATED_UNION"),
    refusedCase(
      "union fallback",
      z.discriminatedUnion("kind", [optionA, optionB], { unionFallback: true }),
      "UNDISCRIMINATED_UNION",
    ),
    refusedCase("container union", z.union([z.array(z.string()), z.string()]), "MIXED_UNION"),
    refusedCase("int float union", z.union([z.int(), z.number()]), "INT_FLOAT_UNION"),
    refusedCase("record key", z.record(z.int(), z.string()), "RECORD_KEY"),
    refusedCase("engine key", z.object({ __h_x: z.string() }), "ENGINE_KEY"),
    refusedCase("intersection", z.intersection(z.object({}), z.object({})), "INTERSECTION"),
    refusedCase("optional item", z.array(z.string().optional()), "OPTIONAL_NOT_A_MEMBER"),
    refusedCase("changing default", z.number().default(changingDefault()), "CHANGING_DEFAULT"),
    refusedCase("recursive", recursive, "RECURSIVE"),
    refusedCase("tuple rest", z.tuple([z.string()], z.string()), "TUPLE_REST"),
    refusedCase("literal member", z.literal(1n), "LITERAL_MEMBER"),
    refusedCase("union option", z.discriminatedUnion("kind", [convertedA, optionB]), "UNION_OPTION"),
  ];
}
