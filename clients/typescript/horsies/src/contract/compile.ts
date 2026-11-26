// The compiled form of a contract: the kind tree of its input and of its output, checked. `Horsies.create` compiles
// each listed contract; a read of a contract that is not listed compiles it at the first read. A contract compiles
// once per process.
//
// A contract with any refusal never reaches the codec: so a default that the writer refuses (`DefaultNotStored`) is
// reported here, and the reader never meets it. The zod adapter (`src/zod/kind.ts`) imports no zod module, so this
// module imports it statically: a bundled producer compiles its zod contracts as an unbundled one does.

import { ROOT_PATH } from "../codec/refusal.ts";
import { checkKind, checkParameters, type KindRefused, type KindRule } from "../wire/check.ts";
import type { Kind, Member, ObjectKind } from "../wire/kinds.ts";
import { objectKind } from "../wire/kinds.ts";
import { isResultOfKind } from "../wire/result-of.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import { kindOf, presenceOf } from "../wire/wire-type.ts";
import { type ZodAdapterRefused, zodMemberWire, zodValueWire } from "../zod/kind.ts";
import type { ZodVersion } from "../zod/version.ts";
import {
  contractRecord,
  type ContractRecord,
  type DefinitionRefused,
  type ParameterSource,
  type SchemaSource,
  type TaskNameFault,
} from "./definition.ts";

/** Why a contract cannot be a task. */
export type ContractRule =
  /** A task name that cannot be stored: empty, or with a lone surrogate. */
  | "TaskName"
  /** A parameter name that is not an identifier, that the engine fills, or that only the engine writes. */
  | "ParameterName"
  /** A Standard Schema that is not a zod schema. */
  | "SchemaVendor"
  /** A zod 3 schema: zod 3.24 and later, or the `zod/v3` export of zod 4. */
  | "Zod3Schema"
  /** A schema of a zod 4 release outside the supported range. */
  | "ZodVersionNotSupported"
  /** A zod form with no stored form. */
  | "ZodSchema"
  /** A wire type that `checkKind` refuses, or an output that may be left out. */
  | "WireKind";

/**
 * A refusal of a contract: the rule, the place (`name`, `input.order.items[*]`, `output`), the cause, and how to fix
 * it (zero or more lines).
 */
export type ContractRefused = {
  readonly rule: ContractRule;
  readonly path: string;
  readonly detail: string;
  readonly help: readonly string[];
};

/** How a contract can be sent. */
export type SendForm =
  /** `app.send` takes the plain parameters: the object kind of every parameter that is not an upstream outcome. */
  | { readonly kind: "sendable"; readonly parameters: ObjectKind }
  /** The task takes an upstream outcome that a sender cannot leave out: only a workflow node runs it. */
  | { readonly kind: "workflowNodeOnly"; readonly upstream: readonly string[] };

/** A contract with checked kind trees. Frozen. */
export type CompiledContract = {
  readonly record: ContractRecord;
  /** Every parameter, upstream outcomes included: the form that a worker reads. */
  readonly parameters: ObjectKind;
  readonly send: SendForm;
  readonly output: Kind;
};

/** The outcome of a compile. */
export type CompileOutcome = Result<CompiledContract, readonly ContractRefused[]>;

/** The rules of `checkParameters` that judge a parameter's name. The definition's name rules judge a name first. */
const NAME_RULES: ReadonlySet<KindRule> = new Set<KindRule>(["EngineKeyMember", "IntegerLikeMember"]);

const compiled = new WeakMap<object, CompileOutcome>();

/**
 * The compiled form of a contract, or every refusal of it in order: the definition (the task name, the parameter
 * names), then the parameters, then the output. A refusal of the definition does not stop the checks of the schemas.
 * Effects: none apart from the kept outcome. A value that `defineTask` did not build is a defect: `TypeError`.
 */
export function compileContract(contract: object): CompileOutcome {
  const kept = compiled.get(contract);

  if (kept !== undefined) {
    return kept;
  }

  const outcome = compileRecord(contractRecord(contract));

  compiled.set(contract, outcome);

  return outcome;
}

function compileRecord(record: ContractRecord): CompileOutcome {
  const members = record.parameters.map((parameter) => compileParameter(parameter));
  const output = compileOutput(record.output);
  const parameters = objectKind(members.flatMap((member) => (member.ok ? [member.value] : [])));

  const refusals = [
    ...record.refusals.map(definitionRefused),
    ...members.flatMap((member) => (member.ok ? [] : member.error)),
    ...parameterKindRefusals(parameters, refusedNamePaths(record.refusals)),
    ...(output.ok ? checkKind(output.value).map((refused) => wireKindRefused("output", refused)) : output.error),
  ];

  if (refusals.length > 0 || !output.ok) {
    return err(Object.freeze(refusals));
  }

  return ok(Object.freeze({ record, parameters, send: sendForm(parameters), output: output.value }));
}

function definitionRefused(refused: DefinitionRefused): ContractRefused {
  switch (refused.rule) {
    case "TaskName":
      return contractRefused("TaskName", refused.path, taskNameDetail(refused.fault), [taskNameHelp(refused.fault)]);
    case "ParameterName":
      return contractRefused("ParameterName", refused.path, refused.detail, []);
    default:
      return unreachable(refused, "definitionRefused");
  }
}

function taskNameDetail(fault: TaskNameFault): string {
  switch (fault) {
    case "empty":
      return "the task name is empty";
    case "loneSurrogate":
      return "the task name holds a lone surrogate (text that is not well formed)";
    default:
      return unreachable(fault, "taskNameDetail");
  }
}

function taskNameHelp(fault: TaskNameFault): string {
  switch (fault) {
    case "empty":
      return "give a task name that is not empty";
    case "loneSurrogate":
      return "give a task name of well-formed text";
    default:
      return unreachable(fault, "taskNameHelp");
  }
}

/** The places of the parameters whose name the definition refused (`input.<name>`). Pure. */
function refusedNamePaths(refusals: readonly DefinitionRefused[]): ReadonlySet<string> {
  return new Set(refusals.flatMap((refused) => (refused.rule === "ParameterName" ? [refused.path] : [])));
}

/**
 * The `checkParameters` refusals of the compiled parameters. A name rule at a parameter whose name the definition
 * refused is left out: the definition's refusal reports that name. Pure.
 */
function parameterKindRefusals(parameters: ObjectKind, refusedNames: ReadonlySet<string>): readonly ContractRefused[] {
  return checkParameters(parameters).flatMap((refused) => {
    const kindRefusal = wireKindRefused("input", refused);

    return NAME_RULES.has(refused.rule) && refusedNames.has(kindRefusal.path) ? [] : [kindRefusal];
  });
}

function compileParameter(parameter: ParameterSource): Result<Member, readonly ContractRefused[]> {
  const path = `input.${parameter.name}`;

  switch (parameter.schema.kind) {
    case "wire": {
      const wire = parameter.schema.wire;

      return ok({ name: parameter.name, kind: kindOf(wire), presence: presenceOf(wire) });
    }

    case "zod": {
      const member = zodMemberWire(parameter.schema.schema);

      return member.ok
        ? ok({ name: parameter.name, kind: kindOf(member.value), presence: presenceOf(member.value) })
        : err(zodRefusals(path, member.error));
    }

    case "otherVendor":
      return err([vendorRefused(path, parameter.schema.vendor)]);
    default:
      return unreachable(parameter.schema, "compileParameter");
  }
}

function compileOutput(source: SchemaSource): Result<Kind, readonly ContractRefused[]> {
  switch (source.kind) {
    case "wire":
      return presenceOf(source.wire).mode === "required"
        ? ok(kindOf(source.wire))
        : err([
            contractRefused("WireKind", "output", "an output is always present", [
              "remove .optional() and .default(): they are for parameters",
            ]),
          ]);
    case "zod": {
      const output = zodValueWire(source.schema);

      return output.ok ? ok(kindOf(output.value)) : err(zodRefusals("output", output.error));
    }

    case "otherVendor":
      return err([vendorRefused("output", source.vendor)]);
    default:
      return unreachable(source, "compileOutput");
  }
}

function vendorRefused(path: string, vendor: string): ContractRefused {
  return contractRefused("SchemaVendor", path, `a Standard Schema of the vendor "${vendor}" has no stored form`, [
    "use h.* or zod 4",
  ]);
}

/** The adapter's refusals as contract refusals at `path`: the adapter's kind is the rule, its fix is the help. */
function zodRefusals(path: string, refused: ZodAdapterRefused): readonly ContractRefused[] {
  switch (refused.kind) {
    case "Zod3Schema":
      return [contractRefused("Zod3Schema", path, "a zod 3 schema", [refused.fix])];
    case "ZodVersionNotSupported":
      return [
        contractRefused(
          "ZodVersionNotSupported",
          path,
          `zod ${versionText(refused.found)} is outside the supported range ${refused.supported}`,
          [`install a zod release in the range ${refused.supported}`],
        ),
      ];
    case "KindRefused":
      return refused.refusals.map((kind) => wireKindRefused(path, kind));
    case "SchemaRefused":
      return refused.refusals.map((node) =>
        contractRefused(
          "ZodSchema",
          `${path}${node.path.slice(ROOT_PATH.length)}`,
          `zod type ${node.zodType} is refused [${node.rule}]`,
          [node.fix],
        ),
      );
    default:
      return unreachable(refused, "zodRefusals");
  }
}

function versionText(version: ZodVersion): string {
  return `${version.major}.${version.minor}.${version.patch}`;
}

/** A `checkKind` refusal under `root` (`input`, `output`, or a parameter). Pure. */
function wireKindRefused(root: string, refused: KindRefused): ContractRefused {
  return contractRefused("WireKind", `${root}${refused.path.slice(ROOT_PATH.length)}`, `${refused.detail} [${refused.rule}]`, []);
}

/** The send form of the parameters. Pure. */
function sendForm(parameters: ObjectKind): SendForm {
  const upstream = parameters.members.filter((member) => isResultOfKind(member.kind));
  const required = upstream.filter((member) => member.presence.mode === "required").map((member) => member.name);

  if (required.length > 0) {
    return Object.freeze({ kind: "workflowNodeOnly", upstream: Object.freeze(required) });
  }

  return Object.freeze({
    kind: "sendable",
    parameters: objectKind(parameters.members.filter((member) => !isResultOfKind(member.kind))),
  });
}

function contractRefused(rule: ContractRule, path: string, detail: string, help: readonly string[]): ContractRefused {
  return Object.freeze({ rule, path, detail, help: Object.freeze([...help]) });
}
