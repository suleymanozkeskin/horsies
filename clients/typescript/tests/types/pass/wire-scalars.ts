// Compile-pass cases: the value type of each scalar wire type.

import { h } from "../../../horsies/src/index.ts";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

function expectType<T extends true>(): T | undefined {
  return undefined;
}

const nothing = h.null();
const flag = h.boolean();
const text = h.string();
const count = h.int();
const large = h.bigint();
const ratio = h.float();

expectType<Equal<h.Infer<typeof nothing>, null>>();

expectType<Equal<h.Infer<typeof flag>, boolean>>();

expectType<Equal<h.Infer<typeof text>, string>>();

expectType<Equal<h.Infer<typeof count>, number>>();

expectType<Equal<h.Infer<typeof large>, bigint>>();

expectType<Equal<h.Infer<typeof ratio>, number>>();
