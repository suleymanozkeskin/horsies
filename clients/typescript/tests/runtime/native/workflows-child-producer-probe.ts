// The test exports of a workflow start, a resume and a workflow handle read in a task child, on the stand-in of the child
// host's producer handle (`child-producer-probe.ts`). Only a build with the cargo feature `test-probes` exports them
// (`bun run --cwd bridge build:dev`); a published addon does not.

import { requireAddon } from "../../../horsies/src/native/call/addon.ts";
import type { NativeEnvelope } from "../../../horsies/src/native/call/envelope.ts";
import type { NativeCoreHandle } from "../../../horsies/src/native/handle/addon.ts";
import type { TraceArgument } from "../../../horsies/src/native/values/records.ts";
import type {
  HandleChange,
  NativeClientSpec,
  WorkflowStartOutcome,
  WorkflowStatusRead,
} from "../../../horsies/src/native/workflows/addon.ts";
import type { NativeProbeChildProducer } from "./child-producer-probe.ts";

/** The workflow test functions of the producer handle of a task child. */
export interface WorkflowsChildProducerProbeAddon {
  probeStartWorkflowInChild(
    handle: NativeCoreHandle,
    child: NativeProbeChildProducer,
    spec: NativeClientSpec,
    workflowId: string,
    sentAtMicros: bigint,
    trace: TraceArgument,
    resendOnTransientErr: boolean,
  ): Promise<NativeEnvelope<WorkflowStartOutcome>>;
  probeResumeWorkflowInChild(
    handle: NativeCoreHandle,
    child: NativeProbeChildProducer,
    workflowId: string,
  ): Promise<NativeEnvelope<HandleChange>>;
  probeWorkflowStatusInChild(
    handle: NativeCoreHandle,
    child: NativeProbeChildProducer,
    workflowId: string,
  ): Promise<NativeEnvelope<WorkflowStatusRead>>;
}

/** The workflow test functions of a task child. Throws when the addon was built without `test-probes`. */
export function workflowsChildProducerProbeAddon(): WorkflowsChildProducerProbeAddon {
  const exports = requireAddon();

  if (!("probeWorkflowStatusInChild" in exports)) {
    throw new Error(
      "the addon exports no probeWorkflowStatusInChild: build it with the cargo feature test-probes (build:dev)",
    );
  }

  // SAFETY: a build with `test-probes` registers these functions with these signatures; the declaration test of the
  // `workflows` area compares this interface with the declarations that the build writes.
  return exports as WorkflowsChildProducerProbeAddon;
}
