// `defineTask`: the contract of a task. A contract holds everything that a sender needs. It has no dependencies and
// does no I/O, so an API server can import it without the worker's code.
//
// `defineTask` builds a frozen contract and keeps its facts (`definition.ts`). It refuses nothing with a throw: what it
// finds wrong (a task name that cannot be stored, a parameter name, a schema of another vendor, a wire type that
// `checkKind` refuses) is a contract refusal, which `Horsies.create` reports. A body gets the result constructors `ok`
// and `err` of its contract in its second argument (`ResultConstructors`), so a wrong value or code fails typecheck
// at its own line.

import { BUILT_IN_TASK_CODES, RESERVED_NON_TASK_CODES } from "../codec/bounds.ts";
import type { JsonValue } from "../codec/json-value.ts";
import type { NonEmpty } from "../wire/builders.ts";
import { type Err, err as errOf, type Ok, ok as okOf } from "../wire/result.ts";
import type { BuiltInTaskCode } from "../wire/task-error.ts";
import { recordDefinition } from "./definition.ts";
import type { ParameterSchemas, OutputSchema, SchemaSent } from "./schema.ts";

/** How a worker runs a task. `ISOLATED` (the default): one task at a time in its own child process. */
export type ExecutionClass = "ISOLATED" | "SHARED_ASYNC" | "SHARED_THREAD";

/** A code that horsies reserves: a built-in task code, or `WORKFLOW_STOPPED`. */
export type ReservedCode = BuiltInTaskCode | (typeof RESERVED_NON_TASK_CODES)[number];

/** A custom code: a code that a task may choose. A built-in name fails typecheck with this text as the expected type. */
export type CustomCode<C extends string> = C extends ReservedCode ? `horsies: "${C}" is a built-in error code` : C;

/** The element type of an `errors` list whose codes the types cannot know (`string[]`): no code fits it. */
export type WidenedCodes = "horsies: declare errors as literal codes, not as a string[]";

/**
 * The error that a body returns: a custom code of the task, an optional message and optional data. `data: undefined`
 * is no data, as a left-out `data` is: `err` gives an error with no `data` member.
 */
export type CustomFailure<E extends string> = {
  readonly code: E;
  readonly message?: string;
  readonly data?: JsonValue;
};

/** The codes that a retry policy names: the task's custom codes and the built-in codes. */
export type RetryCodes<E extends string> = NonEmpty<E | BuiltInTaskCode>;

/** When a failed run runs again. `jitter` is `true` when left out, as in the Python client. */
export type RetryPolicy<E extends string> =
  | {
      readonly kind: "exponential";
      readonly baseSeconds: number;
      readonly maxRetries: number;
      readonly jitter?: boolean;
      readonly maxDelaySeconds?: number;
      readonly autoRetryFor: RetryCodes<E>;
    }
  | {
      readonly kind: "fixed";
      readonly intervalsSeconds: NonEmpty<number>;
      readonly jitter?: boolean;
      readonly autoRetryFor: RetryCodes<E>;
    };

/**
 * The time rules of an execution class: `SHARED_ASYNC` and `SHARED_THREAD` need `timeoutMs`; `SHARED_THREAD` takes no
 * `cancelGraceMs`.
 */
export type ExecutionRules<X extends ExecutionClass> = [X] extends ["SHARED_ASYNC"]
  ? { readonly timeoutMs: number; readonly cancelGraceMs?: number }
  : [X] extends ["SHARED_THREAD"]
    ? { readonly timeoutMs: number; readonly cancelGraceMs?: never }
    : { readonly timeoutMs?: number; readonly cancelGraceMs?: number };

/** Whether a task takes the workflow context: its runtime then has `workflowContext`, and a node may name `ctxFrom`. */
export type ContextUse = "takesContext" | "noContext";

/** The context use of the `workflowContext` field of `defineTask`. */
export type ContextUseOf<C extends boolean> = [C] extends [true] ? "takesContext" : "noContext";

/** The fields of `defineTask`. */
export interface TaskDefinition<
  P extends ParameterSchemas,
  O extends OutputSchema,
  E extends string,
  X extends ExecutionClass,
  C extends boolean = boolean,
> {
  /** The task name: each send stores it, and a worker finds the implementation by it. */
  readonly name: string;
  /** The custom queue of the task. Left out in the default queue mode. */
  readonly queue?: string;
  /** One schema per parameter, stored under its name. `{}` for a task with no parameters. */
  readonly input: P;
  readonly output: O;
  /**
   * The codes that the task may return, as literals (no `as const` needed). Left out: any code that is not a built-in
   * name. A list of the type `string[]` fails typecheck: its codes would not be known to the types.
   */
  readonly errors?: string extends E ? readonly WidenedCodes[] : readonly CustomCode<E>[];
  readonly execution?: X;
  readonly retry?: RetryPolicy<NoInfer<E>>;
  /**
   * The task takes the workflow context: a workflow node of the task may name `ctxFrom`, and the core gives the body
   * the results of those nodes. Left out: no context.
   */
  readonly workflowContext?: C;
}

/** Type-only key of the types of a contract. It does not exist at run time. */
export declare const contractTypes: unique symbol;

/** The types of a contract: its input, output, codes, execution class and context use. */
export interface ContractTypes<
  P extends ParameterSchemas,
  O extends OutputSchema,
  E extends string,
  X extends ExecutionClass,
  C extends ContextUse = ContextUse,
> {
  readonly input: P;
  readonly output: O;
  readonly code: E;
  readonly execution: X;
  readonly context: C;
}

/** The contract of a task. */
export interface TaskContract<
  P extends ParameterSchemas,
  O extends OutputSchema,
  E extends string,
  X extends ExecutionClass,
  C extends ContextUse = ContextUse,
> {
  readonly kind: "task";
  readonly name: string;
  readonly [contractTypes]: ContractTypes<P, O, E, X, C>;
}

/** Any contract: the constraint of a generic parameter. */
export type AnyTaskContract = TaskContract<ParameterSchemas, OutputSchema, string, ExecutionClass>;

/** The result of a body: the success, or an error with a code of the contract. */
export type BodyResult<O extends OutputSchema, E extends string> = Ok<SchemaSent<O>> | Err<CustomFailure<E>>;

/**
 * The result constructors of a contract, in the second argument of its body: `ok(value)` with a value of the output
 * type, and `err({ code, message?, data? })` with a code of the contract. A code that the contract does not declare
 * fails typecheck at the line of the `err` call, and so does a built-in name.
 */
export type ResultConstructors<O extends OutputSchema, E extends string> = {
  /** The success of the body: the output value. */
  readonly ok: (value: SchemaSent<O>) => Ok<SchemaSent<O>>;
  /**
   * The error of the body: a code of the contract. A built-in name throws `TypeError`; in a body that is stored as
   * `UNHANDLED_EXCEPTION`.
   */
  readonly err: <const K extends E>(failure: CustomFailure<CustomCode<K>>) => Err<CustomFailure<E>>;
};

/** The codes that `err` refuses at run time: the built-in names and the reserved names. */
const RESERVED_CODES: ReadonlySet<string> = new Set<string>([...BUILT_IN_TASK_CODES, ...RESERVED_NON_TASK_CODES]);

/**
 * The error of a body, frozen. `data: undefined` is left out: the error holds no `data` member. Throws `TypeError` for
 * a reserved code (a defect that the static type refuses for a literal).
 */
function failureOf<E extends string>(taskName: string, failure: CustomFailure<E>): Err<CustomFailure<E>> {
  if (RESERVED_CODES.has(failure.code)) {
    throw new TypeError(`err of ${taskName}: "${failure.code}" is a built-in error code; a task cannot return it`);
  }

  const { data, ...withoutData } = failure;

  return Object.freeze(errOf(Object.freeze(data === undefined ? withoutData : { ...withoutData, data })));
}

/**
 * The result constructors of the task `taskName`, frozen. Pure. Not exported from an entry point: the implementation
 * keeps them, and the body adapter gives them to the body with the members of its runtime.
 */
export function resultConstructorsOf<O extends OutputSchema, E extends string>(
  taskName: string,
): ResultConstructors<O, E> {
  return Object.freeze({
    ok: (value: SchemaSent<O>): Ok<SchemaSent<O>> => Object.freeze(okOf(value)),
    err: <const K extends E>(failure: CustomFailure<CustomCode<K>>): Err<CustomFailure<E>> =>
      // SAFETY: `CustomCode<K>` is `K` for every code that is not reserved, and `K extends E`; a reserved code throws.
      failureOf(taskName, failure as CustomFailure<E>),
  });
}

/**
 * The contract of a task. Pure: no check that can fail and no I/O; `Horsies.create` reports what the definition
 * breaks.
 */
export function defineTask<
  const P extends ParameterSchemas,
  O extends OutputSchema,
  const E extends string = string,
  const X extends ExecutionClass = "ISOLATED",
  const C extends boolean = false,
>(
  definition: TaskDefinition<P, O, E, X, C> & ExecutionRules<NoInfer<X>>,
): TaskContract<P, O, E, X, ContextUseOf<C>> {
  const contract = { kind: "task", name: definition.name };

  // SAFETY: `contractTypes` is a type-only key; the contract's types are the type parameters of this call.
  const frozen = Object.freeze(contract) as TaskContract<P, O, E, X, ContextUseOf<C>>;

  recordDefinition(frozen, definition);

  return frozen;
}
