// The test export of the addon: `probeParseWorkerManifest`. Only a build with the cargo feature `test-probes`
// exports it (`bun run --cwd bridge build:dev`); a published addon does not.

import { requireAddon } from "../../../horsies/src/native/call/addon.ts";
import type { NativeEnvelope } from "../../../horsies/src/native/call/envelope.ts";
import { nativeValue } from "../../../horsies/src/native/call/envelope.ts";

/** How the core's parse of a worker manifest text ended. */
export type WorkerManifestProbe =
  | { readonly kind: "parsed" }
  | { readonly kind: "refused"; readonly fieldPath: string; readonly cause: string };

/** The test function of the addon. */
export interface ManifestProbeAddon {
  probeParseWorkerManifest(text: string): NativeEnvelope<WorkerManifestProbe>;
}

/** The core's parse of the whole worker manifest `text`. Throws when the addon was built without `test-probes`. */
export function parseWorkerManifestText(text: string): WorkerManifestProbe {
  const exports = requireAddon();

  if (!("probeParseWorkerManifest" in exports)) {
    throw new Error("the addon exports no probeParseWorkerManifest: build it with the cargo feature test-probes (build:dev)");
  }

  // SAFETY: a build with `test-probes` registers this function with this signature; the declaration test of the
  // `worker` area compares this interface with the declarations that the build writes.
  const addon = exports as ManifestProbeAddon;

  return nativeValue("probeParseWorkerManifest", () => addon.probeParseWorkerManifest(text));
}
