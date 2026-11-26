// The check of one node's input, as the worker reads it: each parameter of the task (or of the child workflow) takes a
// value or, for an `h.resultOf` parameter, an upstream node. The static members are encoded with an object kind of the
// plain parameters only, with a read-back; a member that a node fills is left out of the stored input, and the core
// merges the node's result envelope into it at run time.

import { ErrorCode } from "../_catalog/error_codes.ts";
import type { JsonText } from "../codec/json-text.ts";
import { encodeParametersWithReadBack } from "../codec/read-back.ts";
import { encodeParameters } from "../codec/writer.ts";
import { refusalText } from "../codec/refusal.ts";
import { isPlainObject, setOwn } from "../codec/values.ts";
import type { Kind, Member, ObjectKind } from "../wire/kinds.ts";
import { objectKind } from "../wire/kinds.ts";
import type { NonEmpty } from "../wire/builders.ts";
import { isResultOfKind } from "../wire/result-of.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import { type BuildSession, nodeSlot } from "./builder.ts";
import type { CheckedJoin, SpecFinding } from "./findings.ts";
import { finding } from "./findings.ts";
import { storedFormFits } from "./kind-match.ts";

/** The stored input of a node with no static member: `{}`, as the codec's writer writes it. */
export const NO_STATIC_MEMBERS: JsonText = noStaticMembers();

function noStaticMembers(): JsonText {
  const written = encodeParameters(objectKind([]), {});

  if (!written.ok) {
    throw new TypeError("noStaticMembers: the writer refuses an empty input");
  }

  return written.value;
}

/**
 * What a source node gives to an `h.resultOf` parameter: an output of a known kind (a task's output, a child
 * workflow's declared output), or one that the checks do not know (a contract that the app does not list, a finding
 * of its own).
 */
export type SourceOutput = { readonly kind: "known"; readonly output: Kind } | { readonly kind: "notKnown" };

/** The facts of one node input check. */
export type NodeInputFacts = {
  /** The place of the node in findings: `nodes[2] charge`. */
  readonly place: string;
  /** Every parameter of the task or of the child, upstream outcomes included. */
  readonly parameters: ObjectKind;
  readonly input: object;
  readonly join: CheckedJoin;
  readonly session: BuildSession;
  /** The output of the node at an index of the build. */
  readonly sourceOutput: (index: number) => SourceOutput;
};

/** The checked input of a node: the stored static members and the `args_from` sources, in declaration order. */
export type CheckedInput = {
  readonly staticInput: JsonText;
  readonly argsFrom: ReadonlyMap<string, number>;
};

/** The parts of one input before the encode of its static members. */
type InputParts = { readonly statics: Record<string, unknown>; readonly argsFrom: Map<string, number> };

/**
 * The checked input of a node, or every finding of it: an unknown member (`HRS-019`), a missing parameter
 * (`HRS-020`), a node for a plain parameter or a value for an `h.resultOf` one or a source of another output type
 * (`HRS-024`), a required node-fed parameter under an `any` or `quorum` join (`HRS-013`), and static members that do
 * not encode and read back (`HRS-031`). Pure.
 */
export function checkNodeInput(facts: NodeInputFacts): Result<CheckedInput, NonEmpty<SpecFinding>> {
  if (!isPlainObject(facts.input)) {
    return err([
      finding(ErrorCode.WORKFLOW_INVALID_KWARG_KEY, `${facts.place} input`, "the input is not a plain object", []),
    ]);
  }

  const findings = unknownMembers(facts, facts.input);
  const parts: InputParts = { statics: {}, argsFrom: new Map() };

  for (const member of facts.parameters.members) {
    findings.push(...memberFindings(facts, facts.input, member, parts));
  }

  const [first, ...rest] = findings;

  if (first !== undefined) {
    return err([first, ...rest]);
  }

  const plain = objectKind(facts.parameters.members.filter((member) => !isResultOfKind(member.kind)));
  const encoded = encodeParametersWithReadBack(plain, parts.statics);

  if (!encoded.ok) {
    return err([
      finding(ErrorCode.WORKFLOW_KWARGS_NOT_SERIALIZABLE, `${facts.place} input`, refusalText(encoded.error), [
        "give each parameter a value of its declared type",
      ]),
    ]);
  }

  return ok({ staticInput: encoded.value, argsFrom: parts.argsFrom });
}

function unknownMembers(facts: NodeInputFacts, input: Readonly<Record<string, unknown>>): SpecFinding[] {
  return Object.keys(input)
    .filter((key) => !facts.parameters.byName.has(key))
    .map((key) =>
      finding(
        ErrorCode.WORKFLOW_INVALID_KWARG_KEY,
        `${facts.place} input`,
        `the member ${JSON.stringify(key)} is not a parameter`,
        ["give only the parameters that the task or the child workflow declares"],
      ),
    );
}

/** The findings of one parameter; an accepted value goes into `parts`. */
function memberFindings(
  facts: NodeInputFacts,
  input: Readonly<Record<string, unknown>>,
  member: Member,
  parts: InputParts,
): readonly SpecFinding[] {
  const place = `${facts.place} input.${member.name}`;

  if (!Object.hasOwn(input, member.name)) {
    return member.presence.mode === "required"
      ? [
          finding(
            ErrorCode.WORKFLOW_MISSING_REQUIRED_PARAMS,
            place,
            "a required parameter has no value and no node",
            [],
          ),
        ]
      : [];
  }

  const value = input[member.name];
  const slot = value instanceof Object ? nodeSlot(value) : { kind: "notANode" as const };

  if (!isResultOfKind(member.kind)) {
    if (slot.kind === "node") {
      return [
        finding(ErrorCode.WORKFLOW_ARGS_FROM_TYPE_MISMATCH, place, "a plain parameter takes a value, not a node", [
          "declare the parameter h.resultOf(T) to take an upstream node",
        ]),
      ];
    }

    setOwn(parts.statics, member.name, value);

    return [];
  }

  if (slot.kind !== "node") {
    return [
      finding(
        ErrorCode.WORKFLOW_ARGS_FROM_TYPE_MISMATCH,
        place,
        "an h.resultOf parameter takes an upstream node, not a value",
        [],
      ),
    ];
  }

  parts.argsFrom.set(member.name, slot.index);

  return upstreamFindings(facts, member, slot.index, place);
}

/** The findings of a node given to an `h.resultOf` parameter: its output type, and the join rule. */
function upstreamFindings(
  facts: NodeInputFacts,
  member: Member,
  source: number,
  place: string,
): readonly SpecFinding[] {
  const findings: SpecFinding[] = [];
  const output = facts.sourceOutput(source);

  switch (output.kind) {
    case "known":
      if (!storedFormFits(output.output, upstreamInner(member.kind))) {
        findings.push(
          finding(
            ErrorCode.WORKFLOW_ARGS_FROM_TYPE_MISMATCH,
            place,
            "the source node's output type is not the parameter's type",
            [],
          ),
        );
      }

      break;
    case "notKnown":
      break;
    default:
      return unreachable(output, "upstreamFindings");
  }

  if (facts.join.kind !== "all" && member.presence.mode === "required") {
    findings.push(
      finding(ErrorCode.WORKFLOW_INVALID_JOIN, place, `under a ${facts.join.kind} join the source may not have ended`, [
        "declare the parameter .optional(), or join on all",
      ]),
    );
  }

  return findings;
}

/** The kind of the success value of an `h.resultOf` parameter kind. Pure. */
function upstreamInner(kind: Kind): Kind {
  switch (kind.node) {
    case "resultOf":
      return kind.inner;
    case "nullable":
      return upstreamInner(kind.inner);
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
    case "checked":
    case "conversion":
    case "literal":
    case "union":
    case "taggedUnion":
    case "array":
    case "tuple":
    case "record":
    case "object":
      throw new TypeError(`upstreamInner: the kind ${kind.node} is not an upstream outcome`);
    default:
      return unreachable(kind, "upstreamInner");
  }
}
