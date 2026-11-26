// The owner-signed allow list of `unknown` places (`unknown-allowed.json`) and the verdict of the no-`any` check.

import { isArray, isPlainObject, isString } from "../../horsies/src/codec/values.ts";
import type { Result } from "../../horsies/src/wire/result.ts";
import type { ParsedJson } from "./package.ts";
import type { TopTypePlace } from "./surface.ts";

/** A place where the owner allows `unknown`: the declaration path and the form, with the role and the model name. */
export type AllowedPlace = {
  readonly path: string;
  readonly form: string;
  readonly role: string;
  readonly model: string;
};

export type AllowListRefused =
  | { readonly kind: "NoPlaces" }
  | { readonly kind: "PlaceForm"; readonly index: number }
  | { readonly kind: "RepeatedPlace"; readonly path: string; readonly form: string };

const PLACE_KEYS = ["path", "form", "role", "model"] as const;

function placeOf(entry: ParsedJson): AllowedPlace | "not a place" {
  if (!isPlainObject(entry) || Object.keys(entry).length !== PLACE_KEYS.length) {
    return "not a place";
  }

  const [path, form, role, model] = PLACE_KEYS.map((key) => entry[key]);

  if (!isString(path) || !isString(form) || !isString(role) || !isString(model)) {
    return "not a place";
  }

  return { path, form, role, model };
}

/** The places of the allow list document: each with exactly its four texts, none twice. Pure. */
export function allowListOf(document: ParsedJson): Result<readonly AllowedPlace[], AllowListRefused> {
  const entries = isPlainObject(document) ? document["places"] : undefined;

  if (!isArray(entries)) {
    return { ok: false, error: { kind: "NoPlaces" } };
  }

  const places: AllowedPlace[] = [];

  for (const [index, entry] of entries.entries()) {
    const place = placeOf(entry);

    if (place === "not a place") {
      return { ok: false, error: { kind: "PlaceForm", index } };
    }

    if (places.some((known) => known.path === place.path && known.form === place.form)) {
      return { ok: false, error: { kind: "RepeatedPlace", path: place.path, form: place.form } };
    }

    places.push(place);
  }

  return { ok: true, value: places };
}

/** The text of a refused allow list, for a message. */
export function allowListRefusedText(refusal: AllowListRefused): string {
  switch (refusal.kind) {
    case "NoPlaces":
      return "unknown-allowed.json has no list of places";
    case "PlaceForm":
      return `unknown-allowed.json: place ${refusal.index} does not hold exactly path, form, role and model texts`;
    case "RepeatedPlace":
      return `unknown-allowed.json: the place ${refusal.path} (${refusal.form}) is listed twice`;
  }
}

/** The verdict of the top types: it passes when `any` and `unallowed` are empty. */
export type TopTypeVerdict = {
  /** Every `any` in the declaration files: each one fails. */
  readonly any: readonly TopTypePlace[];
  /** Each `unknown` of the public surface that is not on an allowed place: each one fails. */
  readonly unallowed: readonly TopTypePlace[];
  /** The allowed places that hold an `unknown` of the public surface. */
  readonly used: readonly AllowedPlace[];
  /** The allowed places that the built surface does not hold (not built yet). */
  readonly unused: readonly AllowedPlace[];
};

function isAllowed(place: TopTypePlace, allowed: AllowedPlace): boolean {
  return place.path === allowed.path && place.form === allowed.form;
}

/** The verdict of the `any` places of the package and the `unknown` places of the public surface. Pure. */
export function judgeTopTypes(
  anyPlaces: readonly TopTypePlace[],
  unknownPlaces: readonly TopTypePlace[],
  allowList: readonly AllowedPlace[],
): TopTypeVerdict {
  return {
    any: anyPlaces,
    unallowed: unknownPlaces.filter((place) => !allowList.some((allowed) => isAllowed(place, allowed))),
    used: allowList.filter((allowed) => unknownPlaces.some((place) => isAllowed(place, allowed))),
    unused: allowList.filter((allowed) => !unknownPlaces.some((place) => isAllowed(place, allowed))),
  };
}

/** Whether the verdict passes: no `any`, and every `unknown` on an allowed place. Pure. */
export function topTypesPass(verdict: TopTypeVerdict): boolean {
  return verdict.any.length === 0 && verdict.unallowed.length === 0;
}
