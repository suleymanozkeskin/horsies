// The dev-only entry of the addon loader: tests and dev builds load a build of
// `clients/typescript/bridge` from the file that `HORSIES_NATIVE_ADDON_PATH` names.
//
// Not in the published package: `tsconfig.build.json` leaves this file out, and the npm package
// check refuses a packed file that names the variable. The published loader (`load.ts`) loads only
// the platform package of the host.
//
// A process loads the addon once: call `requireDevAddon` (or `loadDevAddon`) before the first call
// that needs the addon. A load of another source that came first is kept, and this entry reports it.

import { isAbsolute } from "node:path";

import { keptLoad, loadedFrom, openAddonFile } from "./load.ts";
import type { AddonLoad } from "./load.ts";

/** The environment variable that names the dev build of the addon: the absolute path of its `.node` file. */
export const DEV_ADDON_PATH_VARIABLE = "HORSIES_NATIVE_ADDON_PATH";

/** The dev addon file that the variable names, or why it names none. */
export type DevAddonPath =
  | { readonly kind: "named"; readonly path: string }
  | { readonly kind: "notSet" }
  | { readonly kind: "notAbsolute"; readonly path: string };

/**
 * The value of the variable as a dev addon path. A relative path is refused: it would resolve against
 * the working folder of the process. Pure.
 */
export function devAddonPath(value: string | undefined): DevAddonPath {
  if (value === undefined) {
    return { kind: "notSet" };
  }

  return isAbsolute(value) ? { kind: "named", path: value } : { kind: "notAbsolute", path: value };
}

/** The outcome of a dev addon load. */
export type DevAddonLoad =
  | { readonly kind: "loaded"; readonly exports: object; readonly path: string }
  | { readonly kind: "pathNotSet" }
  | { readonly kind: "pathNotAbsolute"; readonly path: string }
  | { readonly kind: "outsideMainThread"; readonly threadId: number }
  | { readonly kind: "loadFailed"; readonly path: string; readonly cause: string }
  | { readonly kind: "otherLoadKept"; readonly kept: AddonLoad };

/**
 * Load the dev addon that `variableValue` (the value of `DEV_ADDON_PATH_VARIABLE`) names, at the first
 * load of the process. A refused path loads nothing. Effects: as `keptLoad` with the open of the file.
 */
export function loadDevAddon(variableValue: string | undefined): DevAddonLoad {
  const named = devAddonPath(variableValue);

  switch (named.kind) {
    case "notSet":
      return { kind: "pathNotSet" };
    case "notAbsolute":
      return { kind: "pathNotAbsolute", path: named.path };
    case "named":
      return devLoadOf(named.path, keptLoad(() => loadedFrom({ kind: "devBuild", path: named.path }, openAddonFile(named.path))));
  }
}

/** The dev addon outcome of the kept load: a load of another source, or of another path, is `otherLoadKept`. Pure. */
export function devLoadOf(path: string, kept: AddonLoad): DevAddonLoad {
  switch (kept.kind) {
    case "outsideMainThread":
      return { kind: "outsideMainThread", threadId: kept.threadId };
    case "platformUnsupported":
      return { kind: "otherLoadKept", kept };
    case "loaded":
    case "loadFailed":
      if (kept.source.kind !== "devBuild" || kept.source.path !== path) {
        return { kind: "otherLoadKept", kept };
      }

      return kept.kind === "loaded"
        ? { kind: "loaded", exports: kept.exports, path }
        : { kind: "loadFailed", path, cause: kept.cause };
  }
}

/** The text of a dev addon outcome that is not `loaded`, for a thrown error. Pure. */
export function devLoadFailureText(load: Exclude<DevAddonLoad, { kind: "loaded" }>): string {
  switch (load.kind) {
    case "pathNotSet":
      return `set ${DEV_ADDON_PATH_VARIABLE} to the absolute path of a dev build of the addon (bun run --cwd bridge build:dev)`;
    case "pathNotAbsolute":
      return `${DEV_ADDON_PATH_VARIABLE} must be an absolute path; it is ${JSON.stringify(load.path)}`;
    case "outsideMainThread":
      return `the dev addon loads only on the main thread; this is thread ${load.threadId}`;
    case "loadFailed":
      return `the dev addon did not load from ${load.path}: ${load.cause}`;
    case "otherLoadKept":
      return `this process loaded the addon from another source first: ${keptSourceText(load.kept)}`;
  }
}

function keptSourceText(kept: AddonLoad): string {
  switch (kept.kind) {
    case "loaded":
    case "loadFailed":
      return kept.source.kind === "devBuild" ? kept.source.path : kept.source.packageName;
    case "outsideMainThread":
      return `thread ${kept.threadId}`;
    case "platformUnsupported":
      return `the platform package of ${kept.host}`;
  }
}

/**
 * Load the dev addon that `DEV_ADDON_PATH_VARIABLE` names and give its export object. Throws an `Error`
 * with the cause when it does not load: a test or a dev run cannot go on without it. Effects: reads
 * the variable; as `loadDevAddon`.
 */
export function requireDevAddon(): object {
  const load = loadDevAddon(process.env[DEV_ADDON_PATH_VARIABLE]);

  if (load.kind === "loaded") {
    return load.exports;
  }

  throw new Error(devLoadFailureText(load));
}
