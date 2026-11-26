import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { type AllowedPlace, allowListOf, allowListRefusedText, judgeTopTypes, topTypesPass } from "./allow-list.ts";
import type { TopTypePlace } from "./surface.ts";

const ANY_WIRE: AllowedPlace = {
  path: "AnyWire",
  form: "Wire<unknown, unknown, Presence, Role>",
  role: "Constraint: every wire type",
  model: "AnyWire",
};

const NODE: AllowedPlace = { path: "AnyNode", form: "NodeRef<unknown, string, string>", role: "Constraint", model: "AnyNode" };

function place(keyword: "any" | "unknown", path: string, form: string): TopTypePlace {
  return { keyword, path, form, file: "dist/wire/infer.d.ts", line: 3 };
}

describe("allowListOf", () => {
  test("the places of the list, each with its four texts", () => {
    expect(allowListOf({ rule: "r", places: [ANY_WIRE, NODE] })).toEqual({ ok: true, value: [ANY_WIRE, NODE] });
  });

  test("the committed list holds the eight places of the type-level model", () => {
    const file = fileURLToPath(new URL("unknown-allowed.json", import.meta.url));
    const list = allowListOf(JSON.parse(readFileSync(file, "utf8")));

    expect(list.ok && list.value.map((entry) => entry.model)).toEqual([
      "AnyWire",
      "AnyValueWire",
      "OutputWire",
      "AnyNode",
      "StateEntry",
      "CheckedState",
      "NodeCode<N>",
      "NodeById<N, Id>",
    ]);
  });

  test("NoPlaces: a document without a list of places", () => {
    expect(allowListOf({ rule: "r" })).toEqual({ ok: false, error: { kind: "NoPlaces" } });
  });

  test("PlaceForm: a place without its four texts, or with another key", () => {
    for (const entry of [{ path: "A", form: "F", role: "R" }, { ...ANY_WIRE, reason: "x" }, { ...ANY_WIRE, form: 1 }]) {
      const refused = allowListOf({ places: [ANY_WIRE, entry] });

      expect(refused).toEqual({ ok: false, error: { kind: "PlaceForm", index: 1 } });
      expect(!refused.ok && allowListRefusedText(refused.error)).toContain("place 1");
    }
  });

  test("RepeatedPlace: the same path and form twice", () => {
    expect(allowListOf({ places: [ANY_WIRE, { ...ANY_WIRE, role: "another" }] })).toEqual({
      ok: false,
      error: { kind: "RepeatedPlace", path: ANY_WIRE.path, form: ANY_WIRE.form },
    });
  });
});

describe("judgeTopTypes", () => {
  test("an unknown on an allowed place passes; an allowed place with no unknown is unused", () => {
    const verdict = judgeTopTypes([], [place("unknown", ANY_WIRE.path, ANY_WIRE.form)], [ANY_WIRE, NODE]);

    expect(verdict).toEqual({ any: [], unallowed: [], used: [ANY_WIRE], unused: [NODE] });
    expect(topTypesPass(verdict)).toBe(true);
  });

  test("an unknown outside the list fails, also on an allowed path with another form", () => {
    const outside = place("unknown", "readValue.value", "unknown");
    const otherForm = place("unknown", ANY_WIRE.path, "Wire<unknown>");
    const verdict = judgeTopTypes([], [outside, otherForm], [ANY_WIRE]);

    expect(verdict.unallowed).toEqual([outside, otherForm]);
    expect(topTypesPass(verdict)).toBe(false);
  });

  test("any fails at every place", () => {
    const any = place("any", "looseRead", "any");
    const verdict = judgeTopTypes([any], [], [ANY_WIRE]);

    expect(verdict.any).toEqual([any]);
    expect(topTypesPass(verdict)).toBe(false);
  });
});
