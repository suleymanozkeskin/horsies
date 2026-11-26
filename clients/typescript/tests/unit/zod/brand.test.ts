// The brand of the hz.* schemas: the adapter finds an hz.* codec and hz.wire() by the brand on the function that the
// schema's definition holds (key `Symbol.for("horsies.zod.codec")`), not by the identity of the function. So the brand
// holds for each schema that zod derives from an hz.* schema, and for a brand that another copy of horsies wrote.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { z } from "zod";

import { h } from "../../../horsies/src/wire/builders.ts";
import { STRING_KIND, UUID_KIND } from "../../../horsies/src/wire/kinds.ts";
import { brandHz, HZ_CODEC_BRAND, hzKindOf } from "../../../horsies/src/zod/brand.ts";
import { hz } from "../../../horsies/src/zod/codecs.ts";
import { acceptedKind, acceptedMember } from "./support.ts";
import { kindText, memberText } from "./self-check-cases.ts";

/** A codec with the decode and the encode of `hz.instant()`, built apart from `hz`: zod's own `z.codec()`. */
function codecWith(decode: (text: string) => Temporal.Instant): z.ZodType {
  return z.codec(z.string(), z.custom<Temporal.Instant>(), { decode, encode: (value) => value.toString() });
}

describe("the hz.* brand", () => {
  test("the key is Symbol.for(\"horsies.zod.codec\"), on the decode function of each hz.* codec", () => {
    const codecs = [hz.instant(), hz.offsetDateTime(), hz.plainDateTime(), hz.plainDate(), hz.plainTime(), hz.decimal()];

    assert.equal(HZ_CODEC_BRAND, Symbol.for("horsies.zod.codec"));
    assert.deepEqual(
      codecs.map((codec) => {
        const found = hzKindOf(codec._zod.def.transform);

        return found.found === "hz" ? found.kind.node : "none";
      }),
      ["instant", "offsetDateTime", "plainDateTime", "plainDate", "plainTime", "decimal"],
    );
  });

  test("the brand of hz.wire() is on its test function, with the kind of the wire type", () => {
    const found = hzKindOf(hz.wire(h.uuid())._zod.def.fn);

    assert.equal(found.found === "hz" ? found.kind.node : "none", "uuid");
  });

  test("the brand cannot be listed, written or deleted", () => {
    const decode = hz.instant()._zod.def.transform;

    assert.deepEqual(Object.keys(decode), []);
    assert.equal(Reflect.defineProperty(decode, HZ_CODEC_BRAND, { value: 1 }), false);
    assert.equal(Reflect.deleteProperty(decode, HZ_CODEC_BRAND), false);
  });

  test("each schema that zod derives from an hz.* schema keeps the brand: describe, meta, refine, optional", () => {
    assert.equal(kindText(acceptedKind(hz.instant().describe("when")), 0), "instant");
    assert.equal(kindText(acceptedKind(hz.instant().meta({ id: "at" })), 0), "instant");
    assert.equal(kindText(acceptedKind(hz.decimal().refine(() => true)), 0), "checked(decimal)");
    assert.equal(memberText(acceptedMember(hz.plainDate().optional())), "?plainDate");
    assert.equal(kindText(acceptedKind(hz.wire(h.uuid()).describe("id")), 0), "uuid");
  });

  test("a brand that another copy of horsies wrote is read by its key: a kind object of that copy", () => {
    const decode = (text: string): Temporal.Instant => Temporal.Instant.from(text);

    Object.defineProperty(decode, Symbol.for("horsies.zod.codec"), { value: Object.freeze({ node: "instant" }) });

    assert.equal(kindText(acceptedKind(codecWith(decode)), 0), "instant");
  });

  test("a codec function with no brand is a conversion of its input's kind", () => {
    const decode = (text: string): Temporal.Instant => Temporal.Instant.from(text);

    assert.equal(hzKindOf(decode).found, "none");
    assert.equal(kindText(acceptedKind(codecWith(decode)), 0), "conversion(string)");
  });

  test("a brand value that is not a kind is a defect of the code that wrote it: TypeError", () => {
    const decode = (text: string): Temporal.Instant => Temporal.Instant.from(text);

    Object.defineProperty(decode, HZ_CODEC_BRAND, { value: "instant" });

    assert.throws(() => hzKindOf(decode), TypeError);
  });

  test("a second brand on one function is a defect: TypeError", () => {
    const carrier = (): boolean => true;

    brandHz(carrier, UUID_KIND);

    assert.throws(() => brandHz(carrier, STRING_KIND), TypeError);
  });
});
