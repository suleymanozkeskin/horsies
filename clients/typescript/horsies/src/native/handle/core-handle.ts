// The build of a core handle.

import { nativeValue } from "../call/envelope.ts";
import { handleAddon } from "./addon.ts";
import type { HandleProfile, NativeCoreHandle } from "./addon.ts";

/**
 * A core handle of the producer manifest `manifestJson`, with the pool profile of this process.
 * Starts nothing. Throws `ManifestRefused` for a manifest that the core refuses.
 */
export function buildCoreHandle(manifestJson: string, profile: HandleProfile): NativeCoreHandle {
  return nativeValue("buildCoreHandle", () => handleAddon().buildCoreHandle(manifestJson, profile));
}
