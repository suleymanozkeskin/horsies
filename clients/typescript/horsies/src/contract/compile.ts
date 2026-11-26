// The compiled form of a contract: the kind tree of its input and of its output, checked. `Horsies.create` compiles
// each listed contract; a read of a contract that is not listed compiles it at the first read. A contract compiles
// once per process.
//
// A contract with any refusal never reaches the codec: so a default that the writer refuses (`DefaultNotStored`) is
// reported here, and the reader never meets it.

import { ROOT_PATH } from "../codec/refusal.ts";
import { checkKind, checkParameters, type KindRefused } from "../wire/check.ts";
import type { Kind, Member, ObjectKind } from "../wire/kinds.ts";
import { objectKind } from "../wire/kinds.ts";
import { isResultOfKind } from "../wire/result-of.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { kindOf, presenceOf } from "../wire/wire-type.ts";
import { contractRecord, type ContractRecord, type ParameterSource, type SchemaSource } from "./definition.ts";
import { type ZodAdapterRefused, zodEntry, type ZodKindEntry, type ZodVersion } from "./zod-entry.ts";

/** Why a contract cannot be a task. */
export type ContractRule =
  /** A Standard Schema that is not a zod schema. */
  | "SchemaVendor"
  /** A zod schema of a release that the adapter does not read (zod 3, or a version outside the supported range). */
  | "ZodRelease"
  /** A parameter name that is not an identifier, that the engine fills, or that only the engine writes. */
  | "ParameterName"
  /** A wire type that `checkKind` refuses, or an output that may be left out. */
  | "WireKind"
  /** A zod form with no stored form. */
  | "ZodSchema"
  /** The installed zod reads its known schemas to other kinds than horsies expects. */
  | "ZodSelfCheck"
  /** The zod adapter did not load. */
  | "ZodAdapterNotLoaded";

/** A refusal of a contract: the rule, the place (`input.order.items[*]`, `output`) and the cause. */
export type ContractRefused = { readonly rule: ContractRule; readonly path: string; readonly detail: string };

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

const compiled = new WeakMap<object, CompileOutcome>();

/**
 * The compiled form of a contract, or every refusal of it in order: the definition, then the parameters, then the
 * output. Effects: the first compile of a contract with a zod schema loads the zod adapter. A value that
 * `defineTask` did not build is a defect: `TypeError`.
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
  if (record.refusals.length > 0) {
    return err(Object.freeze(record.refusals.map((refused) => contractRefused(refused.rule, refused.path, detailOf(refused)))));
  }

  const members = record.parameters.map((parameter) => compileParameter(parameter));
  const output = compileOutput(record.output);
  const refusals = [...members.flatMap((member) => (member.ok ? [] : member.error)), ...(output.ok ? [] : output.error)];

  if (refusals.length > 0 || !output.ok) {
    return err(Object.freeze(refusals));
  }

  const parameters = objectKind(members.flatMap((member) => (member.ok ? [member.value] : [])));

  const kindRefusals = [
    ...checkParameters(parameters).map((refused) => wireKindRefused("input", refused)),
    ...checkKind(output.value).map((refused) => wireKindRefused("output", refused)),
  ];

  if (kindRefusals.length > 0) {
    return err(Object.freeze(kindRefusals));
  }

  return ok(Object.freeze({ record, parameters, send: sendForm(parameters), output: output.value }));
}

function detailOf(refused: ContractRecord["refusals"][number]): string {
  switch (refused.rule) {
    case "SchemaVendor":
      return `a Standard Schema of the vendor "${refused.vendor}" has no stored form; use h.* or zod 4`;
    case "ParameterName":
      return refused.detail;
    default: {
      const unexpected: never = refused;

      throw new TypeError(`compileContract: a definition refusal outside its type: ${String(unexpected)}`);
    }
  }
}

function compileParameter(parameter: ParameterSource): Result<Member, readonly ContractRefused[]> {
  const path = `input.${parameter.name}`;

  switch (parameter.schema.kind) {
    case "wire": {
      const wire = parameter.schema.wire;

      return ok({ name: parameter.name, kind: kindOf(wire), presence: presenceOf(wire) });
    }

    case "zod": {
      const schema = parameter.schema.schema;
      const member = withEntry(path, (entry) => entry.zodMemberWire(schema));

      return member.ok
        ? ok({ name: parameter.name, kind: kindOf(member.value), presence: presenceOf(member.value) })
        : member;
    }

    default: {
      const unexpected: never = parameter.schema;

      throw new TypeError(`compileContract: a schema source outside its type: ${String(unexpected)}`);
    }
  }
}

function compileOutput(source: SchemaSource): Result<Kind, readonly ContractRefused[]> {
  switch (source.kind) {
    case "wire":
      return presenceOf(source.wire).mode === "required"
        ? ok(kindOf(source.wire))
        : err([contractRefused("WireKind", "output", "an output is always present; .optional() and .default() are for parameters")]);
    case "zod": {
      const schema = source.schema;

      const output = withEntry("output", (entry) => entry.zodValueWire(schema));

      return output.ok ? ok(kindOf(output.value)) : output;
    }

    default: {
      const unexpected: never = source;

      throw new TypeError(`compileContract: a schema source outside its type: ${String(unexpected)}`);
    }
  }
}

/** Run `compile` with the zod adapter, its refusals as contract refusals at `path`. */
function withEntry<T>(
  path: string,
  compile: (entry: ZodKindEntry) => Result<T, ZodAdapterRefused>,
): Result<T, readonly ContractRefused[]> {
  const load = zodEntry();

  switch (load.kind) {
    case "notLoaded":
      return err([contractRefused("ZodAdapterNotLoaded", path, `the zod adapter of horsies did not load: ${load.cause}`)]);
    case "loaded": {
      const outcome = compile(load.entry);

      return outcome.ok ? outcome : err(zodRefusals(path, outcome.error));
    }

    default: {
      const unexpected: never = load;

      throw new TypeError(`compileContract: a load outcome outside its type: ${String(unexpected)}`);
    }
  }
}

function zodRefusals(path: string, refused: ZodAdapterRefused): readonly ContractRefused[] {
  switch (refused.kind) {
    case "Zod3Schema":
      return [contractRefused("ZodRelease", path, `a zod 3 schema; ${refused.fix}`)];
    case "ZodVersionNotSupported":
      return [
        contractRefused(
          "ZodRelease",
          path,
          `zod ${versionText(refused.found)} is outside the supported range ${refused.supported}`,
        ),
      ];
    case "KindRefused":
      return refused.refusals.map((kind) => wireKindRefused(path, kind));
    case "SchemaRefused":
      return refused.refusals.map((node) =>
        contractRefused(
          "ZodSchema",
          `${path}${node.path.slice(ROOT_PATH.length)}`,
          `zod type ${node.zodType} is refused [${node.rule}]; ${node.fix}`,
        ),
      );
    case "SelfCheckFailed":
      return refused.failures.map((failure) =>
        contractRefused(
          "ZodSelfCheck",
          path,
          `the installed zod reads ${failure.name} as ${failure.found}, not ${failure.expected}; use a zod version that horsies supports`,
        ),
      );
    default: {
      const unexpected: never = refused;

      throw new TypeError(`compileContract: a zod refusal outside its type: ${String(unexpected)}`);
    }
  }
}

function versionText(version: ZodVersion): string {
  return `${version.major}.${version.minor}.${version.patch}`;
}

/** A `checkKind` refusal under `root` (`input`, `output`, or a parameter). Pure. */
function wireKindRefused(root: string, refused: KindRefused): ContractRefused {
  return contractRefused("WireKind", `${root}${refused.path.slice(ROOT_PATH.length)}`, `${refused.detail} [${refused.rule}]`);
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

function contractRefused(rule: ContractRule, path: string, detail: string): ContractRefused {
  return Object.freeze({ rule, path, detail });
}
