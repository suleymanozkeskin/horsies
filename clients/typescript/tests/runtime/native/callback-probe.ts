// The test export of the addon: `probeCallback`. Only a build with the cargo feature `test-probes`
// exports it (`bun run --cwd bridge build:dev`); a published addon does not.

import { requireAddon } from "../../../horsies/src/native/call/addon.ts";
import type { NotRunCause } from "../../../horsies/src/native/call/callback.ts";
import type { NativeEnvelope } from "../../../horsies/src/native/call/envelope.ts";

/** How one call of `probeCallback` ended. */
export type CallbackProbe =
  | { readonly kind: "answered"; readonly answer: string }
  | { readonly kind: "notRun"; readonly cause: NotRunCause }
  | { readonly kind: "threw"; readonly callId: bigint; readonly name: string; readonly message: string }
  | { readonly kind: "panicked"; readonly cause: string }
  | { readonly kind: "answerRefused"; readonly cause: string };

/** The test functions of the addon. */
export interface ProbeAddon {
  probeCallback(
    callback: (callId: bigint, argument: string) => void,
    argument: string,
  ): Promise<NativeEnvelope<CallbackProbe>>;
}

/** The test functions of the addon. Throws when the addon was built without `test-probes`. */
export function probeAddon(): ProbeAddon {
  const exports = requireAddon();

  if (!("probeCallback" in exports)) {
    throw new Error("the addon exports no probeCallback: build it with the cargo feature test-probes (build:dev)");
  }

  // SAFETY: a build with `test-probes` registers this function with this signature; the declaration
  // test of the `call` area compares this interface with the declarations that the build writes.
  return exports as ProbeAddon;
}
