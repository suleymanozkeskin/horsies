// `h.resultOf(T)`: a task input parameter that a workflow node fills with the outcome of an upstream task.

import type { RequiredWire, ValueOf } from "./infer.ts";
import type { Kind } from "./kinds.ts";
import { unreachable } from "./unreachable.ts";
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
      return isResultOfKind(kind.inner);
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
    case "literal":
    case "union":
    case "taggedUnion":
    case "array":
    case "tuple":
    case "record":
    case "object":
    case "json":
    case "conversion":
    case "checked":
      return false;
    default:
      return unreachable(kind, "isResultOfKind");
  }
}
