// The zod adapter as the main entry point reaches it: a synchronous `require` of the `horsies/zod` entry point, the
// first time a contract with a zod schema compiles. The main entry point imports no zod module, also as a type, so a
// user without zod loads and typechecks it. Node 26 and Bun load an ES module with `require`, as the same module
// instance that an `import` of it gives (so `hz.*` codecs are found by identity).
//
// The types below restate the adapter's types structurally (`src/zod/kind.ts`, `src/zod/version.ts`,
// `src/zod/rules.ts`): an import of them would reach zod. `tests/unit/contract/zod-entry.test.ts` checks that the
// adapter's functions are assignable to `ZodKindEntry`.

import { createRequire } from "node:module";
import { extname } from "node:path";
import { fileURLToPath } from "node:url";

import type { KindRefused } from "../wire/check.ts";
import type { RequiredWire, ValueWire } from "../wire/infer.ts";
import type { Result } from "../wire/result.ts";
import type { StandardSchemaMarker } from "./schema.ts";

/** A zod release. */
export type ZodVersion = { readonly major: number; readonly minor: number; readonly patch: number };

/** A refusal of a zod schema node (`ZodSchemaRefused`): the rule, the place, zod's definition type and the fix. */
export type ZodNodeRefused = {
  readonly rule: string;
  readonly path: string;
  readonly zodType: string;
  readonly fix: string;
};

/** A known schema of the adapter's self-check that compiled to another kind (`SelfCheckFailure`). */
export type ZodSelfCheckFailure = { readonly name: string; readonly expected: string; readonly found: string };

/** Why the adapter gives no kind for a schema (`ZodAdapterRefused`). */
export type ZodAdapterRefused =
  | { readonly kind: "Zod3Schema"; readonly fix: string }
  | { readonly kind: "ZodVersionNotSupported"; readonly found: ZodVersion; readonly supported: string }
  | { readonly kind: "SelfCheckFailed"; readonly failures: readonly ZodSelfCheckFailure[] }
  | { readonly kind: "SchemaRefused"; readonly refusals: readonly ZodNodeRefused[] }
  | { readonly kind: "KindRefused"; readonly refusals: readonly KindRefused[] };

/**
 * The functions of `horsies/zod` that the contracts call. Each gives a wire type: the contracts read its kind with
 * `kindOf` and its presence with `presenceOf`.
 */
export interface ZodKindEntry {
  zodMemberWire(schema: StandardSchemaMarker): Result<ValueWire, ZodAdapterRefused>;
  zodValueWire(schema: StandardSchemaMarker): Result<RequiredWire, ZodAdapterRefused>;
}

/** The adapter, or why it did not load (zod is not installed, or the package files are incomplete). */
export type ZodEntryLoad =
  | { readonly kind: "loaded"; readonly entry: ZodKindEntry }
  | { readonly kind: "notLoaded"; readonly cause: string };

/** The stem of the `horsies/zod` entry point, relative to this module. */
const ZOD_ENTRY_STEM = "../zod";

type LoadSlot = { readonly kind: "empty" } | { readonly kind: "filled"; readonly load: ZodEntryLoad };

let loadSlot: LoadSlot = { kind: "empty" };

/** The adapter: the first call loads it, every later call gives the kept outcome. Effects: loads modules. */
export function zodEntry(): ZodEntryLoad {
  switch (loadSlot.kind) {
    case "filled":
      return loadSlot.load;
    case "empty": {
      const load = loadEntry();

      loadSlot = { kind: "filled", load };

      return load;
    }

    default: {
      const unexpected: never = loadSlot;

      throw new TypeError(`zodEntry: a load slot outside its type: ${String(unexpected)}`);
    }
  }
}

/**
 * Load the entry point, in the file form of this module (`.ts` in the source tree, `.js` in the package). A throw of
 * the load (zod is not installed) is `notLoaded` with its message.
 */
function loadEntry(): ZodEntryLoad {
  const thisFile = fileURLToPath(import.meta.url);
  const modulePath = fileURLToPath(new URL(`${ZOD_ENTRY_STEM}${extname(thisFile)}`, import.meta.url));

  try {
    // SAFETY: the entry point exports these two functions; `tests/unit/contract/zod-entry.test.ts` checks that their
    // declarations are assignable to `ZodKindEntry`.
    const entry = createRequire(import.meta.url)(modulePath) as ZodKindEntry;

    return { kind: "loaded", entry };
  } catch (cause) {
    return { kind: "notLoaded", cause: cause instanceof Error ? cause.message : String(cause) };
  }
}
