// The test export of the monitoring handler: `setProbeChildSpecSource`. Only a build with the cargo
// feature `test-probes` exports it (`bun run --cwd bridge build:dev`); a published addon does not.
// A handle that the tests build with no app has no child spec source; the probe sets one that
// builds no child.

import { requireAddon } from "../../../horsies/src/native/call/addon.ts";
import { envelopeValue } from "../../../horsies/src/native/call/envelope.ts";
import type { NativeEnvelope } from "../../../horsies/src/native/call/envelope.ts";
import type { NativeCoreHandle } from "../../../horsies/src/native/handle/addon.ts";
import { buildCoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import type { CoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import type { MonitoringHandlerApp } from "../../../horsies/src/web/handler.ts";

/** The test function of the monitoring handler. */
export interface WebProbeAddon {
  setProbeChildSpecSource(handle: NativeCoreHandle): NativeEnvelope<undefined>;
}

/** The test function of the addon. Throws when the addon was built without `test-probes`. */
export function webProbeAddon(): WebProbeAddon {
  const exports = requireAddon();

  if (!("setProbeChildSpecSource" in exports)) {
    throw new Error("the addon exports no setProbeChildSpecSource: build it with the cargo feature test-probes (build:dev)");
  }

  // SAFETY: a build with `test-probes` registers this function with this signature; the declaration
  // test of the web handler compares this interface with the declarations that the build writes.
  return exports as WebProbeAddon;
}

/** A handle of the manifest `manifestJson` with the probe child spec source. */
export function probedHandle(manifestJson: string): CoreHandle {
  const handle = buildCoreHandle(manifestJson, "PRODUCER");

  envelopeValue(webProbeAddon().setProbeChildSpecSource(handle.native));

  return handle;
}

/** The app facts of a test handler over `handle`. */
export function testApp(handle: CoreHandle): MonitoringHandlerApp {
  return { coreHandle: handle, queueNames: ["default"], scheduleNames: [], clientVersion: "0.0.0-test" };
}
