// The workflow start: `app.start` and `app.retryStart`.
//
// A start: the workflow is listed; the workflow id is a UUID (a new one when left out); the params encode with a
// read-back, and the build gets the value that a child build gets from the stored text; the build runs once and passes
// the spec checks; the start observer wraps the core's start (the conversion to spec data, the parse, the write) and
// reads its outcome. Every refusal before the core writes nothing and opens no span. The same id with the same command
// returns the existing run.

import type { CoreCause, NoCause } from "../_catalog/error_detail.ts";
import { refusalText } from "../codec/refusal.ts";
import { isUuidText } from "../codec/values.ts";
import type { WorkflowId } from "../contract/ids.ts";
import { thrownFacts } from "../native/call/thrown-facts.ts";
import type { TraceArgument } from "../native/values/records.ts";
import { preconditionFailed, ruleRefused } from "../send/errors.ts";
import { epochMicrosTowardPast } from "../send/options.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import { runBuild } from "./builder.ts";
import { type CheckedBuild, checkBuild } from "./checks.ts";
import type { SeatRelayFullOutcome } from "../seats/relay-full.ts";
import type { StartCall, WorkflowCore } from "./core.ts";
import { workflowRecord } from "./define.ts";
import { type WorkflowHandle, workflowHandleOf } from "./handle.ts";
import type { NodeTarget } from "./node-checks.ts";
import type { BuildReturn } from "./node.ts";
import { buildParams, type WorkflowRegistry } from "./registry.ts";
import {
  keptRetry,
  type StartEnqueueFailed,
  startErrorOfRaw,
  startValidationFailed,
  type WorkflowStartError,
  withRetry,
} from "./start-error.ts";

/** The options of a start. */
export interface StartOptions {
  /** The id of the run. Left out: a new UUID. The same id with the same params returns the existing run. */
  readonly workflowId?: WorkflowId;
}

/** The facts of a start that an observer may record. */
export type StartSpanFacts = { readonly workflowName: string; readonly workflowId: string };

/** What the core's start of one start call gives: the handle, or the start error, which names its code. */
export type StartCallResult = Result<unknown, { readonly code: string }>;

/** Wraps each start; `run` makes the core's start with the carrier that the observer gives. */
export interface StartObserver {
  /** Run the core's start of one start call inside the observation, and give its outcome unchanged. */
  observeStart<T extends StartCallResult>(facts: StartSpanFacts, run: (trace: TraceArgument) => Promise<T>): Promise<T>;
}

const NO_CARRIER: TraceArgument = Object.freeze({ kind: "noCarrier" });

/** The observer of an app with no tracing: no span, no carrier. */
export const NO_OP_START_OBSERVER: StartObserver = Object.freeze({
  observeStart: async <T extends StartCallResult>(_facts: StartSpanFacts, run: (trace: TraceArgument) => Promise<T>): Promise<T> =>
    run(NO_CARRIER),
});

/** What a start reads from its app. */
export type StartContext = {
  readonly core: WorkflowCore;
  readonly registry: WorkflowRegistry;
  readonly observer: StartObserver;
  /** The sent instant of a new start. */
  readonly now: () => Temporal.Instant;
  /** A new workflow id: a UUID text. */
  readonly newWorkflowId: () => string;
};

type StartOutcome<R extends BuildReturn> = Result<WorkflowHandle<R>, WorkflowStartError<R>>;

/** Start `workflow` with `params`. Effects: the build, and one start through the core. */
export async function startWorkflow<R extends BuildReturn>(
  context: StartContext,
  workflow: object,
  params: object,
  options: StartOptions,
): Promise<StartOutcome<R>> {
  const record = workflowRecord(workflow);
  const workflowId = options.workflowId ?? context.newWorkflowId();

  const refuse = (message: string, cause: CoreCause | NoCause): StartOutcome<R> =>
    err(startValidationFailed(message, record.name, workflowId, cause));

  const listed = context.registry.byContract(workflow);

  if (listed.kind === "notListed") {
    const message = `the workflow ${record.name} is not in the workflows list of this app; add its contract to Horsies.create({ workflows })`;

    return refuse(message, preconditionFailed(message));
  }

  if (!isUuidText(workflowId)) {
    const message = "HRS-032: workflowId is a UUID in the lower-case 8-4-4-4-12 form";

    return refuse(message, ruleRefused(message));
  }

  const built = buildChecked(context.registry, workflow, params);

  if (!built.ok) {
    return refuse(built.error, ruleRefused(built.error));
  }

  const sentAtMicros = epochMicrosTowardPast(context.now());
  const facts: StartFacts = { workflow, workflowName: record.name, workflowId, sentAtMicros, targets: built.value.targets };

  return context.observer.observeStart({ workflowName: record.name, workflowId }, async (trace) =>
    startOutcome<R>(context, await context.core.start({ spec: built.value.spec, workflowId, sentAtMicros, trace }), facts),
  );
}

/** The checked build of `workflow` with `params`, or the message of the first refusal. Effects: the user's build. */
function buildChecked(registry: WorkflowRegistry, workflow: object, given: object): Result<CheckedBuild, string> {
  const listed = registry.byContract(workflow);

  if (listed.kind === "notListed") {
    return err("the workflow is not in the workflows list of this app");
  }

  const params = buildParams(listed.params, given);

  if (!params.ok) {
    return err(`the params do not encode and read back: ${refusalText(params.error)}`);
  }

  const run = runBuild(listed.record.build, params.value);

  switch (run.kind) {
    case "threw": {
      const facts = thrownFacts(run.cause);

      return err(`the build threw ${facts.name}: ${facts.message}`);
    }

    case "thenable":
      return err("the build returned a thenable; a workflow build is sync");
    case "returned": {
      const checked = checkBuild(listed.record, run, registry.view);

      return checked.ok
        ? checked
        : err(
            `the build fails its checks: ${checked.error.map((found) => `${found.code} ${found.place}: ${found.detail}`).join("; ")}`,
          );
    }

    default:
      return unreachable(run, "buildChecked");
  }
}

/** The facts of one start that its outcome and a retry read. */
type StartFacts = {
  readonly workflow: object;
  readonly workflowName: string;
  readonly workflowId: string;
  readonly sentAtMicros: bigint;
  readonly targets: ReadonlyMap<string, NodeTarget>;
};

/** The handle or the start error of a start call. Pure apart from the kept retry. */
function startOutcome<R extends BuildReturn>(
  context: StartContext,
  call: StartCall | SeatRelayFullOutcome,
  facts: StartFacts,
): StartOutcome<R> {
  const refuse = (message: string): StartOutcome<R> =>
    err(startValidationFailed(message, facts.workflowName, facts.workflowId, ruleRefused(message)));

  switch (call.kind) {
    case "textRefused":
      return refuse(`the spec has a string with no stored text at ${call.refused.path}: ${call.refused.detail}`);
    case "specRefused":
      return refuse(`the core refuses the spec data at ${call.fieldPath}: ${call.cause}`);
    case "seatRelayFull":
      return err(call.error);
    case "outcome":
      switch (call.outcome.kind) {
        case "started":
        case "exists":
          return ok(handleOf<R>(context, facts));
        case "failed": {
          const error = startErrorOfRaw(call.outcome.error);

          return err(
            error.code === "ENQUEUE_FAILED" ? withRetry<R>(error, { ...facts, specData: call.specData }) : error,
          );
        }

        default:
          return unreachable(call.outcome, "startOutcome");
      }

    default:
      return unreachable(call, "startOutcome");
  }
}

function handleOf<R extends BuildReturn>(context: StartContext, facts: StartFacts): WorkflowHandle<R> {
  // SAFETY: `facts.workflowId` passed `isUuidText`, the rule of `WorkflowId.parse`.
  const workflowId = facts.workflowId as WorkflowId;

  return workflowHandleOf<R>({
    core: context.core,
    registry: context.registry,
    workflow: facts.workflow,
    workflowId,
    targets: facts.targets,
  });
}

/**
 * Send again the spec data of `ENQUEUE_FAILED`, under the same workflow id and sent instant. A run that the failed
 * attempt wrote comes back as the existing run. An error that no start of this process gave is `VALIDATION_FAILED`.
 */
export async function retryStart<R extends BuildReturn>(
  context: StartContext,
  error: StartEnqueueFailed<R>,
): Promise<StartOutcome<R>> {
  const kept = keptRetry(error);

  if (kept.kind === "missing") {
    const message = "retryStart takes an ENQUEUE_FAILED error that a start of this process gave";

    return err(startValidationFailed(message, error.workflowName, error.workflowId, ruleRefused(message)));
  }

  const retry = kept.retry;

  return context.observer.observeStart({ workflowName: retry.workflowName, workflowId: retry.workflowId }, async (trace) =>
    startOutcome<R>(
      context,
      await context.core.restart({ specData: retry.specData, workflowId: retry.workflowId, sentAtMicros: retry.sentAtMicros, trace }),
      retry,
    ),
  );
}
