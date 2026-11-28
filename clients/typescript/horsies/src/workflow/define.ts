// `defineWorkflow`: the contract of a workflow. It holds the name, the definition key, the params and the build
// function. Like a task contract it does no I/O, so a sender can import it without the worker's code.
//
// `defineWorkflow` refuses no rule of the definition with a throw: `Horsies.create` and `check` report what the
// definition breaks. The params are compiled with the rules of a task input (the same parameter names, wire types and
// zod schemas). A call that leaves out a field that the type requires (untyped code with no `parameters`) is a defect
// of the call: `defineTask` throws `TypeError` for the params contract.
//
// `outputType` is optional, as the output of Python's `WorkflowDefinitionConfig`. A workflow with no `outputType`
// has no output node: `get` gives `NoWorkflowOutput` or the stored error, and a sub-workflow node of it gives
// `NoWorkflowOutput`. The per-node values come from `results()`.

import type { SpecOnError } from "../_catalog/workflow_spec.ts";
import { defineTask } from "../contract/define-task.ts";
import type { BodyInput, IsUpstreamOutcome, OutputSchema, ParameterSchemas, SchemaValue } from "../contract/schema.ts";
import type { EmptyObject, Flat } from "../wire/infer.ts";
import type { ParameterSent, ParameterValue, SenderMayLeaveOut } from "../zod/infer.ts";
import type { NonEmpty } from "../wire/builders.ts";
import type { BuildReturn, BuildReturnWithoutOutput, NodeRef } from "./node.ts";
import type { WorkflowBuilder } from "./builder.ts";
import { NO_WORKFLOW_OUTPUT_WIRE } from "./no-output.ts";

/** What the core does when a node fails: fail the workflow (the default), or pause it for a resume. */
export type OnError = "fail" | "pause";

/** The fields of `defineWorkflow`. `O` is the declared output schema, `never` for a workflow with no `outputType`. */
export interface WorkflowDefinition<P extends ParameterSchemas, O extends OutputSchema> {
  /** The workflow name: each start stores it. Not empty. */
  readonly name: string;
  /** The key under which a sub-workflow node finds this workflow. Not empty; unique in the app. */
  readonly definitionKey: string;
  /** One schema per parameter of the build, with the rules of a task input. `{}` for none. */
  readonly parameters: P;
  /**
   * The schema of the workflow's result, with the rules of a task output. The output node of every build has this
   * type (`HRS-025`), and every read of the workflow's result decodes with it. Left out: the workflow has no output
   * node, and its builds return none.
   *
   * With `O` at `never` the member takes any output schema. A call infers `never` only when the definition misses a
   * required member and has a member of its own (the compiler infers nothing from such an object), so the diagnostic
   * names that member, not `outputType`.
   */
  readonly outputType?: [O] extends [never] ? OutputSchema : O;
  readonly onError?: OnError;
}

/** What a build of a workflow whose declared output value is `V` returns: an output node of that type. */
export type BuildReturnOf<V> = BuildReturn & { readonly output: NodeRef<V, string, string> };

/**
 * What a build of a workflow with the declared output schema `O` returns: an output node of its value type, or no
 * output node when `O` is `never` (no `outputType`).
 */
export type BuildReturnFor<O extends OutputSchema> = [O] extends [never]
  ? BuildReturnWithoutOutput
  : BuildReturnOf<SchemaValue<O>>;

/**
 * The params of one check case: every param of the build, as in the Python and Rust clients. A plain param takes the
 * value that a start gives; an `h.taskOutcome` param takes the outcome that its upstream node would give
 * (`{ kind: "succeeded", value }` or `{ kind: "failed", error }`). An optional or defaulted param may be left out.
 */
export type CheckCase<P extends ParameterSchemas> = keyof P extends never
  ? EmptyObject
  : Flat<
      {
        -readonly [K in keyof P as SenderMayLeaveOut<P[K]> extends true ? never : K]:
          IsUpstreamOutcome<P[K]> extends true ? ParameterValue<P[K]> : ParameterSent<P[K]>;
      } & {
        -readonly [K in keyof P as SenderMayLeaveOut<P[K]> extends true ? K : never]?:
          IsUpstreamOutcome<P[K]> extends true ? ParameterValue<P[K]> : ParameterSent<P[K]>;
      }
    >;

/**
 * The check cases of a workflow: `check` builds the workflow with each one. Required when `parameters` has a key;
 * a workflow with no parameters takes none.
 */
export type CheckCases<P extends ParameterSchemas> = keyof P extends never
  ? { readonly checkCases?: readonly [] }
  : { readonly checkCases: NonEmpty<CheckCase<P>> };

/** Type-only key of the types of a workflow contract. It does not exist at run time. */
export declare const workflowTypes: unique symbol;

/** The params type and the build return type of a workflow. */
export type WorkflowTypes<P extends ParameterSchemas, R extends BuildReturn> = {
  readonly parameters: P;
  readonly build: R;
};

/** The contract of a workflow. */
export interface WorkflowContract<P extends ParameterSchemas, R extends BuildReturn> {
  readonly kind: "workflow";
  readonly name: string;
  readonly definitionKey: string;
  readonly [workflowTypes]: WorkflowTypes<P, R>;
}

/** Any workflow contract: the constraint of a generic parameter. */
export type AnyWorkflowContract = WorkflowContract<ParameterSchemas, BuildReturn>;

/** The build function of a workflow: sync, no I/O, no clock and no random source. It may run more than once. */
export type WorkflowBuild<P extends ParameterSchemas, R extends BuildReturn> = (
  w: WorkflowBuilder,
  parameters: BodyInput<P>,
) => R;

/** The error rule of a definition: a value of `OnError`, or a value outside it (a refusal at create). */
export type OnErrorSetting =
  | { readonly kind: "valid"; readonly onError: SpecOnError }
  | { readonly kind: "invalid"; readonly given: string };

/** Whether a definition declares the workflow's output: `outputType` given, or left out (no output node). */
export type OutputDeclaration = { readonly kind: "declared" } | { readonly kind: "notDeclared" };

/** The facts of one workflow contract. Frozen. */
export type WorkflowRecord = {
  readonly name: string;
  readonly definitionKey: string;
  readonly onError: OnErrorSetting;
  readonly outputDeclaration: OutputDeclaration;
  /**
   * The task contract of the params and the success value of a sub-workflow node of the workflow (task name
   * `PARAMS_CONTRACT_NAME`): its compiled form is the params' kind tree and the output kind. The output is the
   * declared `outputType`, or `NO_WORKFLOW_OUTPUT_WIRE` for a workflow with no `outputType`.
   */
  readonly paramsContract: object;
  /** The parameter names of the params, in declaration order. */
  readonly paramNames: readonly string[];
  /** The check cases as given; none is an empty list. */
  readonly checkCases: readonly object[];
  /** The build. Its parameter types are erased: the package decodes the params with the params' kind tree. */
  readonly build: (w: WorkflowBuilder, params: never) => unknown;
};

/** The fields of a definition as `defineWorkflow` reads them. */
type DefinitionFields = {
  readonly name: string;
  readonly definitionKey: string;
  readonly parameters: ParameterSchemas;
  readonly outputType?: OutputSchema | undefined;
  readonly onError?: string;
  readonly checkCases?: readonly object[];
};

/**
 * The task name of the params contract of every workflow. The contract only carries the params and the declared
 * output to the contract compile, so its name is a fixed label: the workflow's own name rules check the workflow name
 * at `Horsies.create` (`nameRefusals` of `create-checks.ts`).
 */
const PARAMS_CONTRACT_NAME = "workflow params";

const records = new WeakMap<object, WorkflowRecord>();

const OUTPUT_DECLARED: OutputDeclaration = Object.freeze({ kind: "declared" });
const OUTPUT_NOT_DECLARED: OutputDeclaration = Object.freeze({ kind: "notDeclared" });

/**
 * The contract of a workflow. With `outputType`, each build returns an output node of that type; without it, each
 * build returns no output node. Pure apart from the kept facts: no check that can fail and no I/O.
 */
export function defineWorkflow<
  const P extends ParameterSchemas,
  O extends OutputSchema = never,
  const R extends BuildReturnFor<O> = BuildReturnFor<O>,
>(definition: WorkflowDefinition<P, O> & NoInfer<CheckCases<P>>, build: WorkflowBuild<P, R>): WorkflowContract<P, R> {
  const contract = { kind: "workflow", name: definition.name, definitionKey: definition.definitionKey };

  // SAFETY: `workflowTypes` is a type-only key; the contract's types are the type parameters of this call.
  const frozen = Object.freeze(contract) as WorkflowContract<P, R>;

  // SAFETY: the build's parameter type is erased in the record; the package gives it the params decoded with the
  // params' own kind tree, whose value type is `BodyInput<P>`.
  records.set(frozen, recordOf(definition, build as (w: WorkflowBuilder, params: never) => unknown));

  return frozen;
}

/** The facts of a workflow contract. A value that `defineWorkflow` did not build is a defect: `TypeError`. */
export function workflowRecord(workflow: object): WorkflowRecord {
  const kept = records.get(workflow);

  if (kept === undefined) {
    throw new TypeError("horsies: the value is not a workflow contract (build it with defineWorkflow)");
  }

  return kept;
}

/** Whether `defineWorkflow` built the value. */
export function isWorkflowContract(value: object): boolean {
  return records.has(value);
}

function recordOf(fields: DefinitionFields, build: (w: WorkflowBuilder, params: never) => unknown): WorkflowRecord {
  const params: ParameterSchemas = fields.parameters;

  return Object.freeze({
    name: fields.name,
    definitionKey: fields.definitionKey,
    onError: onErrorSetting(fields.onError),
    outputDeclaration: outputDeclarationOf(fields.outputType),
    paramsContract: defineTask({
      name: PARAMS_CONTRACT_NAME,
      input: params,
      output: fields.outputType ?? NO_WORKFLOW_OUTPUT_WIRE,
    }),
    paramNames: Object.freeze(Object.keys(params)),
    checkCases: Object.freeze([...(fields.checkCases ?? [])]),
    build,
  });
}

/** `declared` for a given `outputType`; `notDeclared` when it is left out (or `undefined` from untyped code). Pure. */
function outputDeclarationOf(outputType: OutputSchema | undefined): OutputDeclaration {
  return outputType === undefined ? OUTPUT_NOT_DECLARED : OUTPUT_DECLARED;
}

function onErrorSetting(onError: string | undefined): OnErrorSetting {
  switch (onError) {
    case undefined:
    case "fail":
      return { kind: "valid", onError: "FAIL" };
    case "pause":
      return { kind: "valid", onError: "PAUSE" };
    default:
      return { kind: "invalid", given: onError };
  }
}
