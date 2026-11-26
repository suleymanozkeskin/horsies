// The test exports of the producer handle of a task child: `probeChildProducer`, and the send and the task reads on
// its stand-in. Only a build with the cargo feature `test-probes` exports them (`bun run --cwd bridge build:dev`); a
// published addon does not.

import { requireAddon } from "../../../horsies/src/native/call/addon.ts";
import type { NativeEnvelope } from "../../../horsies/src/native/call/envelope.ts";
import type { NativeCoreHandle } from "../../../horsies/src/native/handle/addon.ts";
import type {
  EnqueueOutcome,
  EnqueueRequest,
  ResultRead,
  SendKind,
  TaskInfoRead,
  WaitBound,
} from "../../../horsies/src/native/sends/addon.ts";
import type { TraceArgument } from "../../../horsies/src/native/values/records.ts";

/** How the close of a stand-in ended: `CLOSED` when an operation connected it, `NEVER_CONNECTED` when none did. */
export type ProbeClose = "NEVER_CONNECTED" | "CLOSED";

/** A stand-in of the child host's producer handle: a core handle of the manifest that the test gives. */
export interface NativeProbeChildProducer {
  close(): Promise<NativeEnvelope<ProbeClose>>;
}

/** The test functions of the producer handle of a task child. */
export interface ChildProducerProbeAddon {
  probeChildProducer(manifestJson: string): NativeEnvelope<NativeProbeChildProducer>;
  probeEnqueueInChild(
    handle: NativeCoreHandle,
    child: NativeProbeChildProducer,
    request: EnqueueRequest,
    sendKind: SendKind,
    trace: TraceArgument,
  ): Promise<NativeEnvelope<EnqueueOutcome>>;
  probeGetResultInChild(
    handle: NativeCoreHandle,
    child: NativeProbeChildProducer,
    taskId: string,
    wait: WaitBound,
  ): Promise<NativeEnvelope<ResultRead>>;
  probeTaskInfoInChild(
    handle: NativeCoreHandle,
    child: NativeProbeChildProducer,
    taskId: string,
  ): Promise<NativeEnvelope<TaskInfoRead>>;
}

/** The test functions of the producer handle of a task child. Throws when the addon was built without `test-probes`. */
export function childProducerProbeAddon(): ChildProducerProbeAddon {
  const exports = requireAddon();

  if (!("probeEnqueueInChild" in exports)) {
    throw new Error("the addon exports no probeEnqueueInChild: build it with the cargo feature test-probes (build:dev)");
  }

  // SAFETY: a build with `test-probes` registers these functions with these signatures; the declaration test of the
  // `sends` area compares this interface with the declarations that the build writes.
  return exports as ChildProducerProbeAddon;
}
