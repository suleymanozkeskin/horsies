// The addon object for the accessors of the areas, or the thrown error of a load that failed.

import { AddonOutsideMainThread, RuntimeStartFailed } from "../../thrown-errors.ts";
import { loadAddon } from "../load.ts";
import type { AddonLoad, AddonSource } from "../load.ts";
import { unreachable } from "./never.ts";

/**
 * The export object of the addon. The first call loads it (`loadAddon`). Throws
 * `AddonOutsideMainThread` in a `worker_threads` isolate, and `RuntimeStartFailed` when no addon file
 * serves the host or the file does not load.
 */
export function requireAddon(): object {
  return addonOrThrow(loadAddon());
}

/** The export object of a load, or the thrown error of a failed load. Pure. */
export function addonOrThrow(load: AddonLoad): object {
  switch (load.kind) {
    case "loaded":
      return load.exports;
    case "outsideMainThread":
      throw new AddonOutsideMainThread({ threadId: load.threadId });
    case "platformUnsupported":
      throw new RuntimeStartFailed({
        cause: `no native addon of horsies is built for ${load.host}`,
      });
    case "loadFailed":
      throw new RuntimeStartFailed({
        cause: `the native addon did not load from ${sourceText(load.source)}: ${load.cause}`,
      });
    default:
      return unreachable(load);
  }
}

function sourceText(source: AddonSource): string {
  switch (source.kind) {
    case "devBuild":
      return `the dev build ${source.path}`;
    case "platformPackage":
      return `the package ${source.packageName}`;
    default:
      return unreachable(source);
  }
}
