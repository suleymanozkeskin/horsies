// `defineTask`: the contract of a task. A contract holds everything that a sender needs. It has no dependencies and
// does no I/O, so an API server can import it without the worker's code.
//
// `defineTask` builds a frozen contract and keeps its facts (`definition.ts`). It refuses nothing with a throw: what it
// finds wrong (a member that is no schema, a schema of another vendor, a wire type that `checkKind` refuses) is a
// definition refusal, which `Horsies.create` reports. The result constructors `ok` and `err` are bound to the
// contract, so a wrong value or code fails typecheck at its own line.

import { LIBRARY_TASK_CODES, RESERVED_NON_TASK_CODES } from "../codec/bounds.ts";
import type { JsonValue } from "../codec/json-value.ts";
import type { NonEmpty } from "../wire/builders.ts";
import { type Err, err as errOf, type Ok, ok as okOf } from "../wire/result.ts";
import type { LibraryTaskCode } from "../wire/task-error.ts";
import { recordDefinition } from "./definition.ts";
import type { ParameterSchemas, OutputSchema, SchemaSent } from "./schema.ts";

/** How a worker runs a task. `ISOLATED` (the default): one task at a time in its own child process. */
export type ExecutionClass = "ISOLATED" | "SHARED_ASYNC" | "SHARED_THREAD";

/** A code that the library reserves: a library task code, or `WORKFLOW_STOPPED`. */
export type ReservedCode = LibraryTaskCode | (typeof RESERVED_NON_TASK_CODES)[number];

/** A code that a task may choose. A built-in name fails typecheck with this text as the expected type. */
export type UserCode<C extends string> = C extends ReservedCode ? `horsies: "${C}" is a built-in error code` : C;

/** The error that a body returns: a code that the task chose, an optional message and optional data. */
export type UserFailure<E extends string> = {
  readonly code: E;
  readonly message?: string;
  readonly data?: JsonValue;
};

/** The codes that a retry policy names: the task's codes and the library's codes. */
export type RetryCodes<E extends string> = NonEmpty<E | LibraryTaskCode>;

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

/** The fields of `defineTask`. */
export interface TaskDefinition<P extends ParameterSchemas, O extends OutputSchema, E extends string, X extends ExecutionClass> {
  /** The task name: each send stores it, and a worker finds the implementation by it. */
  readonly name: string;
  /** The custom queue of the task. Left out in the default queue mode. */
  readonly queue?: string;
  /** One schema per parameter, stored under its name. `{}` for a task with no parameters. */
  readonly input: P;
  readonly output: O;
  /** The codes that the task may return. Left out: any code that is not a built-in name. */
  readonly errors?: readonly UserCode<E>[];
  readonly execution?: X;
  readonly retry?: RetryPolicy<NoInfer<E>>;
}

/** Type-only key of the types of a contract. It does not exist at run time. */
export declare const contractTypes: unique symbol;

/** The types of a contract: its input, output, codes and execution class. */
export interface ContractTypes<P extends ParameterSchemas, O extends OutputSchema, E extends string, X extends ExecutionClass> {
  readonly input: P;
  readonly output: O;
  readonly code: E;
  readonly execution: X;
}

/** The contract of a task. */
export interface TaskContract<P extends ParameterSchemas, O extends OutputSchema, E extends string, X extends ExecutionClass> {
  readonly kind: "task";
  readonly name: string;
  readonly [contractTypes]: ContractTypes<P, O, E, X>;
  /** The success of a body: the output value. */
  ok(value: SchemaSent<O>): Ok<SchemaSent<O>>;
  /**
   * The error of a body: a code of the contract. A built-in name throws `TypeError`; in a body that is stored as
   * `UNHANDLED_EXCEPTION`.
   */
  err<const K extends E>(failure: UserFailure<UserCode<K>>): Err<UserFailure<E>>;
}

/** Any contract: the constraint of a generic parameter. */
export type AnyTaskContract = TaskContract<ParameterSchemas, OutputSchema, string, ExecutionClass>;

/** The result of a body: the success, or an error with a code of the contract. */
export type BodyResult<O extends OutputSchema, E extends string> = Ok<SchemaSent<O>> | Err<UserFailure<E>>;

/** The codes that `err` refuses at run time: the built-in names and the reserved names. */
const RESERVED_CODES: ReadonlySet<string> = new Set<string>([...LIBRARY_TASK_CODES, ...RESERVED_NON_TASK_CODES]);

/** The error of a body. Throws `TypeError` for a reserved code (a defect that the static type refuses for a literal). */
function failureOf<E extends string>(taskName: string, failure: UserFailure<E>): Err<UserFailure<E>> {
  if (RESERVED_CODES.has(failure.code)) {
    throw new TypeError(`${taskName}.err: "${failure.code}" is a built-in error code; a task cannot return it`);
  }

  return Object.freeze(errOf(Object.freeze({ ...failure })));
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
>(definition: TaskDefinition<P, O, E, X> & ExecutionRules<NoInfer<X>>): TaskContract<P, O, E, X> {
  const name = definition.name;

  const contract = {
    kind: "task",
    name,
    ok: (value: SchemaSent<O>): Ok<SchemaSent<O>> => Object.freeze(okOf(value)),
    err: <const K extends E>(failure: UserFailure<UserCode<K>>): Err<UserFailure<E>> =>
      // SAFETY: `UserCode<K>` is `K` for every code that is not reserved, and `K extends E`; a reserved code throws.
      failureOf(name, failure as UserFailure<E>),
  };

  // SAFETY: `contractTypes` is a type-only key; the contract's types are the type parameters of this call.
  const frozen = Object.freeze(contract) as TaskContract<P, O, E, X>;

  recordDefinition(frozen, definition);

  return frozen;
}
