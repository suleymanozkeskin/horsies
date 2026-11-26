// The brand of an `hz.*` schema: the kind that horsies stores for it.
//
// - The brand is on the function that the schema's definition holds: the decode function of an `hz.*` codec
//   (`_zod.def.transform`), the test function of `hz.wire()` (`_zod.def.fn`). zod gives each schema that it derives
//   from a schema the same definition functions (`.describe()`, `.meta()` and `.refine()` clone the schema), so the
//   brand holds for each of them. A property of the schema object would not: a clone is a new object.
// - The key is `HZ_CODEC_BRAND`, from the process-wide symbol registry (`Symbol.for`). So the adapter of one copy of
//   horsies reads the brand that another copy wrote (a bundle with its own copy of `horsies/zod`), and the adapter
//   finds an `hz.*` schema by its brand, not by the identity of its functions.
// - The value is a kind of the kind tree: frozen data that every copy of horsies reads the same.
//
// This module imports no zod module: the adapter in the main entry point reads the brand, `codecs.ts` writes it.

import { isObjectLike, isString } from "../codec/values.ts";
import type { Kind } from "../wire/kinds.ts";

/** The key of the brand: `Symbol.for("horsies.zod.codec")`. */
export const HZ_CODEC_BRAND: symbol = Symbol.for("horsies.zod.codec");

/** The kind that a brand names: found, or the function has no brand. */
export type HzKind = { readonly found: "hz"; readonly kind: Kind } | { readonly found: "none" };

const NOT_HZ: HzKind = Object.freeze({ found: "none" });

/**
 * Brand `carrier`, a function that `codecs.ts` made for one `hz.*` schema, with `kind`. Effects: defines a property
 * that cannot be written, deleted or listed. A second brand on one carrier is a defect: `TypeError`.
 */
export function brandHz(carrier: object, kind: Kind): void {
  Object.defineProperty(carrier, HZ_CODEC_BRAND, { value: kind, enumerable: false, writable: false, configurable: false });
}

/**
 * The kind of the brand on `carrier` (a definition function of a zod schema), or `none`. A brand value that is not a
 * kind is a defect of the code that wrote the key: `TypeError`. Pure.
 */
export function hzKindOf(carrier: object): HzKind {
  if (!Object.hasOwn(carrier, HZ_CODEC_BRAND)) {
    return NOT_HZ;
  }

  const brand: unknown = Reflect.get(carrier, HZ_CODEC_BRAND);

  if (!isObjectLike(brand) || !("node" in brand) || !isString(brand.node)) {
    throw new TypeError("hzKindOf: the value of the horsies.zod.codec brand is not a kind of horsies");
  }

  // SAFETY: only `brandHz` of a copy of horsies writes the brand, with a frozen kind of the kind tree; the node name
  // is checked above.
  return Object.freeze({ found: "hz", kind: brand as Kind });
}
