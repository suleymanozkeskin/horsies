// The implementation of a contract: the body and the map of thrown error classes to codes. `app.implement` builds
// it; only the worker imports the module that calls it.

import type {
  TaskRuntime,
  ThreadTaskRuntime,
  ThreadWorkflowTaskRuntime,
  WorkflowTaskRuntime,
} from "../child/runtime.ts";
import { runImplementation } from "../testing/run.ts";
import type { TaskOutcome } from "../wire/task-error.ts";
import {
  type BodyResult,
  type ContextUse,
  type ExecutionClass,
  type ResultConstructors,
  resultConstructorsOf,
  type TaskContract,
} from "./define-task.ts";
import type { BodyInput, ParameterSchemas, OutputSchema, SchemaValue } from "./schema.ts";

/**
 * The run context that a body gets as its second argument, by execution class: the worker's child process gives it,
 * and `TaskRuntime.forTest` gives one to a test. A `SHARED_THREAD` body has no `spawn`.
 */
export interface BodyRuntimes {
  readonly ISOLATED: TaskRuntime;
  readonly SHARED_ASYNC: TaskRuntime;
  readonly SHARED_THREAD: ThreadTaskRuntime;
}

/**
 * The run context of a body of a task with `workflowContext: true`, by execution class: the runtime of `BodyRuntimes`
 * with the member `workflowContext`.
 */
export interface ContextBodyRuntimes {
  readonly ISOLATED: WorkflowTaskRuntime;
  readonly SHARED_ASYNC: WorkflowTaskRuntime;
  readonly SHARED_THREAD: ThreadWorkflowTaskRuntime;
}

/**
 * The run context of a body of the execution class `X` and the context use `C`: only a task that takes the workflow
 * context (`"takesContext"`) has `workflowContext` in its runtime.
 */
export type RuntimeOf<X extends ExecutionClass, C extends ContextUse = "noContext"> = [C] extends ["takesContext"]
  ? ContextBodyRuntimes[X]
  : BodyRuntimes[X];

/**
 * The body of a task. `SHARED_ASYNC` takes an async body only: its runs share one event loop. `ISOLATED` and
 * `SHARED_THREAD` take a sync or an async body. The second argument is one frozen object per run: the members of the
 * runtime of the class, and the result constructors `ok` and `err` of the contract. Each member is an own property and
 * each function member is bound to its run, so a body may destructure it (`{ ok, err, signal }`).
 */
export type BodyFor<
  P extends ParameterSchemas,
  O extends OutputSchema,
  E extends string,
  X extends ExecutionClass,
  C extends ContextUse = "noContext",
> = [X] extends ["SHARED_ASYNC"]
  ? (input: BodyInput<P>, runtime: RuntimeOf<X, C> & ResultConstructors<O, E>) => Promise<BodyResult<O, E>>
  : (
      input: BodyInput<P>,
      runtime: RuntimeOf<X, C> & ResultConstructors<O, E>,
    ) => BodyResult<O, E> | Promise<BodyResult<O, E>>;

/** A class of thrown errors. */
export type ErrorClass = abstract new (...parameters: never[]) => Error;

/** One entry of the exception map: a thrown error of the class is stored as the code. */
export type ExceptionMapping<E extends string> = readonly [ErrorClass, E];

/** The options of `app.implement`. */
export interface ImplementOptions<E extends string> {
  /**
   * A thrown error of a listed class (or a subclass) is stored as the mapped code, the first entry that matches.
   * Any other thrown error is stored as `UNHANDLED_EXCEPTION`.
   */
  readonly exceptions?: readonly ExceptionMapping<E>[];
}

/** The implementation of a contract. */
export interface Implementation<
  P extends ParameterSchemas,
  O extends OutputSchema,
  E extends string,
  X extends ExecutionClass,
  C extends ContextUse = ContextUse,
> {
  readonly kind: "implementation";
  readonly contract: TaskContract<P, O, E, X, C>;
  /**
   * Run the body with `input` and `runtime` (`TaskRuntime.forTest`), with no worker and no database: the input and
   * the output are checked with the contract's types, as a worker checks them, and a thrown error goes through the
   * exception map. Gives the outcome as a task read gives it. The body gets the own members of `runtime` (its
   * workflow context too) and the contract's `ok` and `err`.
   */
  run(input: BodyInput<P>, runtime: RuntimeOf<X, C>): Promise<TaskOutcome<SchemaValue<O>, E>>;
}

/** Any implementation: the constraint of a generic parameter. */
export type AnyImplementation = Implementation<ParameterSchemas, OutputSchema, string, ExecutionClass>;

/**
 * The parts of an implementation that the worker's child process reads. The body's types are erased: the child
 * decodes the input and checks the output with the contract's kind trees.
 */
export type ImplementationParts = {
  readonly contract: object;
  readonly body: (input: never, runtime: never) => unknown;
  readonly exceptions: readonly (readonly [ErrorClass, string])[];
  /** The `ok` and `err` of the contract, which the body adapter gives the body with the members of its runtime. */
  readonly resultConstructors: ResultConstructors<OutputSchema, string>;
};

const parts = new WeakMap<object, ImplementationParts>();

/** A frozen implementation, with its parts kept out of its public members. Pure apart from the kept parts. */
export function implementationOf<
  P extends ParameterSchemas,
  O extends OutputSchema,
  E extends string,
  X extends ExecutionClass,
  C extends ContextUse,
>(
  contract: TaskContract<P, O, E, X, C>,
  body: BodyFor<P, O, E, X, C>,
  exceptions: readonly ExceptionMapping<E>[],
): Implementation<P, O, E, X, C> {
  const kept: ImplementationParts = Object.freeze({
    contract,
    body,
    exceptions: Object.freeze([...exceptions]),
    resultConstructors: resultConstructorsOf<OutputSchema, string>(contract.name),
  });

  const implementation: Implementation<P, O, E, X, C> = Object.freeze({
    kind: "implementation",
    contract,
    run: async (input: BodyInput<P>, runtime: RuntimeOf<X, C>): Promise<TaskOutcome<SchemaValue<O>, E>> =>
      // SAFETY: the runner decodes the value with the contract's output kind, and a user code of the error is a code
      // of the contract (a declared code, by typecheck of `err` and of the exception map).
      runImplementation(kept, input, runtime) as Promise<TaskOutcome<SchemaValue<O>, E>>,
  });

  parts.set(implementation, kept);

  return implementation;
}

/** The parts of an implementation. A value that `app.implement` did not build is a defect: `TypeError`. */
export function implementationParts(implementation: object): ImplementationParts {
  const kept = parts.get(implementation);

  if (kept === undefined) {
    throw new TypeError("horsies: the value is not an implementation (build it with app.implement)");
  }

  return kept;
}
