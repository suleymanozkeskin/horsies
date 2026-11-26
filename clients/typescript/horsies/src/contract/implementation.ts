// The implementation of a contract: the body and the map of thrown error classes to codes. `app.implement` builds
// it; only the worker imports the module that calls it.

import type { BodyResult, ExecutionClass, TaskContract } from "./define-task.ts";
import type { BodyInput, ParameterSchemas, OutputSchema } from "./schema.ts";

/**
 * The run context that a body gets as its second argument, by execution class. The worker's child process gives
 * it; its member types join this map with the child process code. A key whose type is `never` has no run context
 * yet: a body of that class takes one argument.
 */
export interface BodyRuntimes {
  readonly ISOLATED: never;
  readonly SHARED_ASYNC: never;
  readonly SHARED_THREAD: never;
}

/** The run context of a body of the execution class `X`. */
export type RuntimeOf<X extends ExecutionClass> = BodyRuntimes[X];

/**
 * The body of a task. `SHARED_ASYNC` takes an async body only: its runs share one event loop. `ISOLATED` and
 * `SHARED_THREAD` take a sync or an async body.
 */
export type BodyFor<P extends ParameterSchemas, O extends OutputSchema, E extends string, X extends ExecutionClass> = [
  X,
] extends ["SHARED_ASYNC"]
  ? (input: BodyInput<P>, runtime: RuntimeOf<X>) => Promise<BodyResult<O, E>>
  : (input: BodyInput<P>, runtime: RuntimeOf<X>) => BodyResult<O, E> | Promise<BodyResult<O, E>>;

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
export interface Implementation<P extends ParameterSchemas, O extends OutputSchema, E extends string, X extends ExecutionClass> {
  readonly kind: "implementation";
  readonly contract: TaskContract<P, O, E, X>;
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
};

const parts = new WeakMap<object, ImplementationParts>();

/** A frozen implementation, with its parts kept out of its public members. Pure apart from the kept parts. */
export function implementationOf<P extends ParameterSchemas, O extends OutputSchema, E extends string, X extends ExecutionClass>(
  contract: TaskContract<P, O, E, X>,
  body: BodyFor<P, O, E, X>,
  exceptions: readonly ExceptionMapping<E>[],
): Implementation<P, O, E, X> {
  const implementation: Implementation<P, O, E, X> = Object.freeze({ kind: "implementation", contract });

  parts.set(implementation, Object.freeze({ contract, body, exceptions: Object.freeze([...exceptions]) }));

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
