// The self-check of the zod adapter: each rule runs once per process on a known schema, as Python's
// `verify_shape_rules` does. zod gives no stability statement for its definition fields; a renamed field makes a
// known schema compile to another kind or another refusal, and the self-check reports it. It never switches a rule
// off: a failure refuses every zod schema until horsies reads the new field.

import { z } from "zod";

import { MAX_VALUE_JSON_DEPTH } from "../codec/bounds.ts";
import type { JsonValue } from "../codec/json-value.ts";
import { encodeValue } from "../codec/writer.ts";
import { h } from "../wire/builders.ts";
import type { Kind, Member, MemberPresence } from "../wire/kinds.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import { compileMember, type ZodMemberKind } from "./adapter.ts";
import { hz } from "./codecs.ts";
import type { ZodSchema } from "./definition.ts";
import type { ZodRule } from "./rules.ts";

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

/** The outcome of a self-check run. */
export type SelfCheckOutcome = Result<true, readonly SelfCheckFailure[]>;

/** The self-check of this process: not run yet, or its outcome. */
type SelfCheckState = { readonly state: "notRun" } | { readonly state: "ran"; readonly outcome: SelfCheckOutcome };

let processState: SelfCheckState = { state: "notRun" };

/**
 * The self-check with the known schemas, once per process: the first call runs it, later calls give its outcome.
 * Fails with the cases whose schema compiled to another kind or refusal (`SelfCheckFailure`).
 */
export function selfCheckOnce(): SelfCheckOutcome {
  switch (processState.state) {
    case "ran":
      return processState.outcome;
    case "notRun": {
      const outcome = runSelfCheck(knownCases());

      processState = { state: "ran", outcome };

      return outcome;
    }

    default:
      return unreachable(processState, "selfCheckOnce");
  }
}

/** The outcome of compiling each case and writing its probes. Fails with every case that differs. Pure. */
export function runSelfCheck(cases: readonly SelfCheckCase[]): SelfCheckOutcome {
  const failures = cases.flatMap(caseFailures);

  return failures.length === 0 ? ok(true) : err(Object.freeze(failures));
}

function caseFailures(known: SelfCheckCase): readonly SelfCheckFailure[] {
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
    case "resultOf":
      return `${kind.node}(${inner(kind.inner)})`;
    case "conversion":
      return `conversion(${inner(kind.stored)})`;
    default:
      return kind.node;
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
  return [...acceptedCases(), ...containerCases(), ...refusedCases()];
}

function acceptedCases(): readonly SelfCheckCase[] {
  return [
    kindCase("string", z.string(), "string"),
    kindCase("string format", z.email(), "checked(string)", [{ value: "no", written: "refused" }]),
    kindCase("string check", z.string().max(1), "checked(string)", [{ value: "ab", written: "refused" }]),
    kindCase("int", z.int32(), "checked(int)", [{ value: 2 ** 40, written: "refused" }]),
    kindCase("int check", z.number().int(), "checked(int)"),
    kindCase("float", z.number(), "float"),
    kindCase("bigint", z.bigint(), "bigint"),
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
  const recursive: ZodSchema = z.lazy(() => z.array(recursive));
  const optionA = z.object({ kind: z.literal("a") });
  const convertedA = z.codec(optionA, optionA, { decode: (value) => value, encode: (value) => value });
  const optionB = z.object({ kind: z.literal("b") });

  return [
    refusedCase("any", z.any(), "UNTYPED"),
    refusedCase("transform", z.string().transform((text) => text.length), "TRANSFORM"),
    refusedCase("pipe", z.string().pipe(z.string()), "TRANSFORM"),
    refusedCase("trim", z.string().trim(), "TRANSFORM"),
    refusedCase("catch", z.string().catch(""), "CATCH"),
    refusedCase("date", z.date(), "DATE"),
    refusedCase("loose object", z.looseObject({}), "LOOSE_OBJECT"),
    refusedCase("catchall", z.object({}).catchall(z.string()), "LOOSE_OBJECT"),
    refusedCase("map", z.map(z.string(), z.string()), "NOT_DATA"),
    refusedCase("custom", z.custom<string>(), "NOT_DATA"),
    refusedCase("object union", z.union([z.object({}), z.string()]), "UNDISCRIMINATED_UNION"),
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
