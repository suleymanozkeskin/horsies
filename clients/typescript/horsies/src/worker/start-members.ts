// The members of the worker manifest that the start gives, not the app config: the profile, the metrics endpoint,
// the tasks, the event handlers, the worker-ended spans switch and the child launch. The addon builds the config
// sections from the app's accepted config text and parses the whole manifest with the core's parser
// (`prepareWorker`).

import {
  childLaunchManifestToWire,
  type ChildLaunchManifest,
  eventHandlersToWire,
  type EventHandlers,
  metricsEndpointToWire,
  type MetricsEndpoint,
  profileRequestToWire,
  type ProfileRequest,
  taskManifestToWire,
  type TaskManifest,
  type WorkerEndedSpans,
  type WorkerLogLevel,
} from "../_catalog/worker_manifest.ts";
import type { WireJson } from "../_catalog/wire_json.ts";
import type { AppContext } from "../app/context.ts";
import type { HandlerRegistration } from "../app/telemetry.ts";
import { contractRecord } from "../contract/definition.ts";
import { unreachable } from "../native/call/never.ts";
import type { TracingSetting } from "../telemetry/tracing-config.ts";
import type { ChildCommand } from "./child-command.ts";
import { manifestText } from "./manifest-text.ts";

/** The start members of one worker. */
export type StartMembers = {
  readonly profile: ProfileRequest;
  readonly metricsEndpoint: MetricsEndpoint;
  readonly tasks: ReadonlyMap<string, TaskManifest>;
  readonly handlers: EventHandlers;
  readonly workerEndedSpans: WorkerEndedSpans;
  readonly childLaunch: ChildLaunchManifest;
};

/** The JSON text of the start members, in the manifest's own member names and forms. Pure. */
export function startMembersText(members: StartMembers): string {
  return manifestText(
    new Map<string, WireJson>([
      ["profile", profileRequestToWire(members.profile)],
      ["metrics_endpoint", metricsEndpointToWire(members.metricsEndpoint)],
      ["tasks", new Map(Array.from(members.tasks, ([name, task]) => [name, taskManifestToWire(task)] as const))],
      ["handlers", eventHandlersToWire(members.handlers)],
      ["worker_ended_spans", members.workerEndedSpans],
      ["child_launch", childLaunchManifestToWire(members.childLaunch)],
    ]),
  );
}

/**
 * The facts of each implemented task, by task name, in registration order: the execution class of its contract and
 * the user error codes that the contract declares (none for a contract with open codes). Pure.
 */
export function taskManifests(context: AppContext): ReadonlyMap<string, TaskManifest> {
  return new Map(
    Array.from(context.implementations, ([name, parts]) => {
      const record = contractRecord(parts.contract);
      const codes = record.codes.kind === "declared" ? [...record.codes.codes] : [];

      return [name, { execution: record.execution, error_codes: codes }] as const;
    }),
  );
}

/** The handler member: `NO_HANDLERS` for no handler, else the name and event type of each, in order. Pure. */
export function eventHandlers(handlers: readonly HandlerRegistration[]): EventHandlers {
  return handlers.length === 0
    ? { kind: "NO_HANDLERS" }
    : { kind: "HANDLERS", handlers: handlers.map((handler) => ({ name: handler.name, event_type: handler.eventType })) };
}

/**
 * Whether the handler child makes the span of each attempt that the worker ended: with OpenTelemetry tracing and its
 * `workerEndedSpans` switch on. Pure.
 */
export function workerEndedSpans(tracing: TracingSetting): WorkerEndedSpans {
  switch (tracing.kind) {
    case "otel":
      return endedSpansOfSwitch(tracing.workerEndedSpans);
    case "none":
      return "NO_SPANS";
    default:
      return unreachable(tracing);
  }
}

function endedSpansOfSwitch(spans: "makeSpans" | "noSpans"): WorkerEndedSpans {
  switch (spans) {
    case "makeSpans":
      return "MAKE_SPANS";
    case "noSpans":
      return "NO_SPANS";
    default:
      return unreachable(spans);
  }
}

/**
 * How each child starts: `command`, no preload process, the host-child program value `RUST` (the worker program in
 * child mode: the core's child host serves the JavaScript child), and the log level of the start. Pure.
 */
export function childLaunch(command: ChildCommand, logLevel: WorkerLogLevel): ChildLaunchManifest {
  return {
    command: { program: command.program, args: [...command.args] },
    preload: { kind: "NO_PRELOAD_PROGRAM" },
    program: { kind: "RUST" },
    log_level: logLevel,
  };
}
