// `h.resultOf(T)`: a task input parameter that a workflow node fills with the outcome of an upstream task.

import type { RequiredWire, ValueOf } from "./infer.ts";
import type { Kind } from "./kinds.ts";
import type { TaskOutcome } from "./task-error.ts";
import { kindOf, type Wire, wireOf } from "./wire-type.ts";

/** The wire type of an upstream outcome. A sender never gives it (its sent type is `never`). */
export type ResultOfWire<W extends RequiredWire> = Wire<TaskOutcome<ValueOf<W>>, never, "required", "resultOf">;

/** The wire type of the outcome of an upstream task whose output has the wire type `inner`. */
export function resultOfWire<W extends RequiredWire>(inner: W): ResultOfWire<W> {
  return wireOf<TaskOutcome<ValueOf<W>>, never, "resultOf">(Object.freeze({ node: "resultOf", inner: kindOf(inner) }));
}

/** Whether a kind holds an upstream outcome at its top: `h.resultOf(T)`, or `h.resultOf(T).nullable()`. Pure. */
export function isResultOfKind(kind: Kind): boolean {
  switch (kind.node) {
    case "resultOf":
      return true;
    case "nullable":
      return kind.inner.node === "resultOf";
    default:
      return false;
  }
}
