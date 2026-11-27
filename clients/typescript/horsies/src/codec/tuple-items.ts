// The items of a tuple kind for the value walks (the writer, the reader, the check view): the item count that an
// array must have, and the kind of each index. A fixed tuple takes exactly its items; a tuple with a rest item takes
// its fixed items and then any number of rest items (pydantic-core's tuple schema with a variadic item).

import type { Kind, TupleKind } from "../wire/kinds.ts";
import { unreachable } from "../wire/unreachable.ts";

/** Whether an array has the item count of a tuple kind, or the rule of the count that it breaks. */
export type TupleLength = { readonly state: "fits" } | { readonly state: "wrongLength"; readonly detail: string };

const FITS: TupleLength = Object.freeze({ state: "fits" });

/** Whether an array of `length` items has the item count of `kind`. Pure. */
export function tupleLength(kind: TupleKind, length: number): TupleLength {
  const fixed = kind.items.length;

  switch (kind.rest.state) {
    case "none":
      return length === fixed ? FITS : wrongLength(`expected an array of ${fixed} items`);
    case "items":
      return length >= fixed ? FITS : wrongLength(`expected an array of at least ${fixed} items`);
    default:
      return unreachable(kind.rest, "tupleLength");
  }
}

function wrongLength(detail: string): TupleLength {
  return Object.freeze({ state: "wrongLength", detail });
}

/**
 * The kind of the item at `index`: a fixed item, or the rest item past the fixed items. The caller checked the item
 * count with `tupleLength`, so an index past a fixed tuple is a defect: `TypeError`. Pure.
 */
export function tupleItem(kind: TupleKind, index: number): Kind {
  const fixed = kind.items[index];

  if (fixed !== undefined) {
    return fixed;
  }

  switch (kind.rest.state) {
    case "items":
      return kind.rest.item;
    case "none":
      throw new TypeError(`tupleItem: the tuple has no item ${index}`);
    default:
      return unreachable(kind.rest, "tupleItem");
  }
}
