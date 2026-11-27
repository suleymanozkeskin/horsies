// The entry of the end-to-end suite that runs the source of `horsies` and the dev build of the addon (a local run:
// `HORSIES_NATIVE_ADDON_PATH` names the file). The lane of the built package selects `package.ts` instead
// (`HORSIES_E2E_ENTRY=package`, see `e2e/run.ts`); the end-to-end files import `#horsies`, which `tests/package.json`
// maps to one of the two.

import { appInternals } from "../../../horsies/src/app/internals.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";

export * from "../../../horsies/src/index.ts";

export { appInternals };

/**
 * Load the addon of this process and give its exports; throws when it does not load. Call it on the main thread before
 * the first call that needs the addon. Effects: loads the dev addon.
 */
export function loadAddon(): object {
  return requireDevAddon();
}
