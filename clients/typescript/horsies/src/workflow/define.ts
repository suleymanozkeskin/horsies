// `defineWorkflow`: the contract of a workflow. It holds the name, the definition key, the params and the build
// function. Like a task contract it does no I/O, so a sender can import it without the worker's code.
//
// `defineWorkflow` refuses nothing with a throw: `Horsies.create` and `check` report what the definition breaks. The
// params are compiled with the rules of a task input (the same parameter names, wire types and zod schemas).

import type { SpecOnError } from "../_catalog/workflow_spec.ts";
import { defineTask } from "../contract/define-task.ts";
import type { BodyInput, OutputSchema, ParameterSchemas, SchemaValue, SendInput } from "../contract/schema.ts";
import type { NonEmpty } from "../wire/builders.ts";
import type { BuildReturn, NodeRef } from "./node.ts";
import type { WorkflowBuilder } from "./builder.ts";

/** What the core does when a node fails: fail the workflow (the default), or pause it for a resume. */
export type OnError = "fail" | "pause";

/** The fields of `defineWorkflow`. */
export interface WorkflowDefinition<P extends ParameterSchemas, O extends OutputSchema> {
  /** The workflow name: each start stores it. Not empty. */
  readonly name: string;
  /** The key under which a sub-workflow node finds this workflow. Not empty; unique in the app. */
  readonly definitionKey: string;
  /** One schema per parameter of the build, with the rules of a task input. `{}` for none. */
  readonly params: P;
  /**
   * The schema of the workflow's result, with the rules of a task output. The output node of every build has this
   * type (`HRS-025`), and every read of the workflow's result decodes with it.
   */
  readonly output: O;
  readonly onError?: OnError;
}

/** What a build of a workflow whose declared output value is `V` returns: an output node of that type. */
export type BuildReturnOf<V> = BuildReturn & { readonly output: NodeRef<V, string, string> };

/**
 * The check cases of a workflow: `check` builds the workflow with each one. Required when `params` has a key; a
 * workflow with no params takes none.
 */
export type CheckCases<P extends ParameterSchemas> = keyof P extends never
  ? { readonly checkCases?: readonly [] }
  : { readonly checkCases: NonEmpty<SendInput<P>> };

/** Type-only key of the types of a workflow contract. It does not exist at run time. */
export declare const workflowTypes: unique symbol;

/** The params type and the build return type of a workflow. */
export type WorkflowTypes<P extends ParameterSchemas, R extends BuildReturn> = {
  readonly params: P;
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
  params: BodyInput<P>,
) => R;

/** The error rule of a definition: a value of `OnError`, or a value outside it (a refusal at create). */
export type OnErrorSetting =
  | { readonly kind: "valid"; readonly onError: SpecOnError }
  | { readonly kind: "invalid"; readonly given: string };

/** The facts of one workflow contract. Frozen. */
export type WorkflowRecord = {
  readonly name: string;
  readonly definitionKey: string;
  readonly onError: OnErrorSetting;
  /**
   * The task contract of the params and the declared output (task name `PARAMS_CONTRACT_NAME`): its compiled form is
   * the params' kind tree and the output kind.
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
  readonly params: ParameterSchemas;
  readonly output: OutputSchema;
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

/** The contract of a workflow. Pure apart from the kept facts: no check that can fail and no I/O. */
export function defineWorkflow<
  const P extends ParameterSchemas,
  O extends OutputSchema,
  const R extends BuildReturnOf<SchemaValue<O>>,
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
  const params: ParameterSchemas = fields.params;

  return Object.freeze({
    name: fields.name,
    definitionKey: fields.definitionKey,
    onError: onErrorSetting(fields.onError),
    paramsContract: defineTask({ name: PARAMS_CONTRACT_NAME, input: params, output: fields.output }),
    paramNames: Object.freeze(Object.keys(params)),
    checkCases: Object.freeze([...(fields.checkCases ?? [])]),
    build,
  });
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
