// The entry of the end-to-end suite that runs the built `horsies` package, imported through its `exports` map, and the
// platform package that the lane installed next to it (`e2e/install-built-package.ts`). The published loader reads no
// path variable. The type checker never reads this file: `tests/package.json` maps `#horsies` to it only under the
// condition `horsies-package`, and the package has no declarations before the lane builds it.

import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { pathToFileURL } from "node:url";

import type * as Source from "./source.ts";

export * from "horsies";

const packageDir = dirname(createRequire(import.meta.url).resolve("horsies/package.json"));

/** A module of the built `dist/` folder: the test code reaches the internals that the exports map hides by file URL. */
async function distModule<Module>(path: string): Promise<Module> {
  // SAFETY: the module is the build of the source module that the caller names as `Module`.
  return (await import(pathToFileURL(join(packageDir, "dist", path)).href)) as Module;
}

const internals = await distModule<{ appInternals: typeof Source.appInternals }>("app/internals.js");
const loader = await distModule<typeof import("../../../horsies/src/native/load.ts")>("native/load.js");

export const appInternals: typeof Source.appInternals = internals.appInternals;

/**
 * Load the addon through the package loader (the platform package of the host) and give its exports; throws when it
 * does not load. Effects: loads the addon at the first call of the process.
 */
export function loadAddon(): object {
  const loaded = loader.loadAddon();

  if (loaded.kind !== "loaded") {
    throw new Error(`the package loader did not load the addon: ${JSON.stringify(loaded)}`);
  }

  return loaded.exports;
}
