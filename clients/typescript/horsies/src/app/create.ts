// `Horsies.create`: the checks of the config and of each listed contract, in order, and the app context.
//
// Order of the refusals: the config (the TypeScript parse and the tracing step, then the core's check), then the
// contracts in list order (the list, the queue, the wire types, then the core's checks of the codes and the options).
//
// The thread rule: the addon loads only on the main thread of a process. On a main thread every check runs; the core's
// checks call the addon's pure check functions, which start no core. On a worker thread only the checks that are
// TypeScript code run, and no addon loads: a seat isolate reaches the core through the relay of its seat, another
// worker thread has no core.

import { isMainThread } from "node:worker_threads";

import { type AppConfig, type AppConfigInput, parseAppConfig, resendOnTransientError } from "../config/app-config.ts";
import { queueSetupOf } from "../config/queues.ts";
import type { EnvironmentRead } from "../config/secret.ts";
import type { AnyTaskContract } from "../contract/define-task.ts";
import { buildCoreHandle } from "../native/handle/core-handle.ts";
import type { NativeTaskOptions } from "../native/sends/addon.ts";
import {
  checkAppConfig,
  checkTaskOptions,
  enqueue,
  getResult,
  parseDeclaredErrorCodes,
  prepareSendFacts,
  processRole,
  taskInfo,
} from "../native/sends/sends.ts";
import { workerThreadCore } from "../seats/channel.ts";
import {
  processTracingEnvironment,
  recordTracingParts,
  type TracingParts,
  tracingPartsOf,
} from "../telemetry/create-hook.ts";
import { type OtelApiLoader, requireOtelApi } from "../telemetry/otel-api.ts";
import { tracingSettingOf } from "../telemetry/tracing-config.ts";
import { registeredHandlers } from "../events/register.ts";
import { ClientDefect } from "../thrown-errors.ts";
import type { QueueSetup } from "../config/queues.ts";
import type { AnyWorkflowContract } from "../workflow/define.ts";
import { admittedQueueNames, mainThreadWorkflowCore } from "../workflow/core.ts";
import { checkWorkflowsAtCreate } from "../workflow/create-checks.ts";
import type { WorkflowRegistry } from "../workflow/registry.ts";
import type { NonEmpty } from "../wire/builders.ts";
import { err, ok, type Result } from "../wire/result.ts";
import type { AppContext } from "./context.ts";
import {
  checkContractList,
  contractLookup as contractLookupOf,
  inListOrder,
  type ListedEntry,
  type ListRefusal,
} from "./contract-list.ts";
import { type AcceptedConfig, checkConfigWithCore, checkTaskWithCore, type CoreChecks } from "./core-checks.ts";
import type { AddonCore, CoreAccess, ProducerCore } from "./core-access.ts";
import type { Refusal } from "./refusal.ts";
import { RefusalSink } from "./refusals.ts";
import { initialSuppression, type ProcessStart, processStart, SuppressionState } from "./suppression.ts";

/** The config of `Horsies.create`: the app config and the contract lists. */
export type HorsiesConfig = AppConfigInput & {
  /** Every task contract of the app. A send of a contract that is not listed is refused. */
  readonly tasks: readonly AnyTaskContract[];
  /** Every workflow contract of the app. A start, or a sub-workflow node, of an unlisted workflow is refused. */
  readonly workflows?: readonly AnyWorkflowContract[];
};

/** Why `Horsies.create` gives no app: every refusal, in order. */
export type CreateRefused = { readonly refusals: NonEmpty<Refusal> };

/** The facts of the process that `Horsies.create` reads. A test gives its own. */
export interface CreateEnvironment {
  readonly mainThread: boolean;
  readonly environment: EnvironmentRead;
  readonly processStart: ProcessStart;
  /** The core's check functions. Called on a main thread only; the first call loads the addon. */
  readonly coreChecks: () => CoreChecks;
  /**
   * The core of the app on a main thread: the operations of its sends, reads and workflows, and its one core handle,
   * on which the app's child spec source is registered.
   */
  readonly addonCore: (
    accepted: AcceptedConfig,
    options: ReadonlyMap<string, NativeTaskOptions>,
    workflows: WorkflowsOnMainThread,
  ) => AddonCore;
  /** The load of the OpenTelemetry API; called only for `tracing: { kind: "otel" }`, on every thread. */
  readonly otelApi: OtelApiLoader;
}

/** The facts of this process. Effects: reads the thread, the environment and the arguments. */
export function processEnvironment(): CreateEnvironment {
  return {
    mainThread: isMainThread,
    environment: (name) => process.env[name],
    processStart: processStart(),
    coreChecks: () => ({ checkAppConfig, parseDeclaredErrorCodes, checkTaskOptions }),
    addonCore: addonCoreOf,
    otelApi: requireOtelApi,
  };
}

/** The app context of `config`, or every refusal in order. */
export function createApp(config: HorsiesConfig, environment: CreateEnvironment): Result<AppContext, CreateRefused> {
  const parsed = parseAppConfig(config, environment.environment);
  const tracing = tracingPartsOf(tracingSettingOf(config.tracing), processTracingEnvironment(environment.otelApi));
  // The queue check reads only the queue fields of the input, so a refused config adds no false queue refusal.
  const queues = queueSetupOf(config);
  const list = checkContractList(config.tasks, queues);

  const workflows = checkWorkflowsAtCreate({
    workflows: config.workflows ?? [],
    tasks: contractLookupOf(list.entries),
    queues,
  });

  if (!parsed.ok || !tracing.ok) {
    return refused([
      ...(parsed.ok ? [] : parsed.error),
      ...(tracing.ok ? [] : [tracing.error]),
      ...inListOrder(list.refusals),
      ...workflows.refusals,
    ]);
  }

  if (!environment.mainThread) {
    return list.refusals.length > 0 || workflows.refusals.length > 0
      ? refused([...inListOrder(list.refusals), ...workflows.refusals])
      : ok(contextOf(parsed.value, list.entries, workerThreadCore(), environment, tracing.value, workflows.registry));
  }

  const checks = environment.coreChecks();
  const accepted = checkConfigWithCore(checks, parsed.value.coreConfigJson);
  const options = coreCheckedOptions(checks, list.entries);

  const refusals = [
    ...(accepted.kind === "refused" ? accepted.refusals : []),
    ...inListOrder([...list.refusals, ...options.refusals]),
    ...workflows.refusals,
  ];

  if (accepted.kind === "refused" || refusals.length > 0) {
    return refused(refusals);
  }

  const onMainThread: WorkflowsOnMainThread = {
    registry: workflows.registry,
    queues,
    resendOnTransientError: resendOnTransientError(parsed.value),
  };

  const core: CoreAccess = { kind: "addon", ...environment.addonCore(accepted.value, options.options, onMainThread) };

  return ok(contextOf(parsed.value, list.entries, core, environment, tracing.value, workflows.registry));
}

/** The core checks of each listed contract, in list order, and the checked options by task name. */
function coreCheckedOptions(
  checks: CoreChecks,
  entries: readonly ListedEntry[],
): { readonly options: ReadonlyMap<string, NativeTaskOptions>; readonly refusals: readonly ListRefusal[] } {
  const options = new Map<string, NativeTaskOptions>();
  const refusals: ListRefusal[] = [];

  for (const entry of entries) {
    const checked = checkTaskWithCore(checks, entry.task.compiled.record, entry.task.route, entry.path);

    if (checked.kind === "accepted") {
      options.set(entry.task.compiled.record.name, checked.value);
    } else {
      refusals.push(...checked.refusals.map((refused) => ({ index: entry.index, refusal: refused })));
    }
  }

  return { options, refusals };
}

function refused(refusals: readonly Refusal[]): Result<AppContext, CreateRefused> {
  const [first, ...rest] = refusals;

  if (first === undefined) {
    throw new TypeError("createApp: a refused create with no refusal");
  }

  const all: NonEmpty<Refusal> = Object.freeze([first, ...rest]);

  return err(Object.freeze({ refusals: all }));
}

function contextOf(
  config: AppConfig,
  listed: readonly ListedEntry[],
  core: CoreAccess,
  environment: CreateEnvironment,
  tracing: TracingParts,
  workflows: WorkflowRegistry,
): AppContext {
  const suppression = new SuppressionState(initialSuppression(environment.processStart), environment.environment);

  const context: AppContext = {
    config,
    listed,
    core,
    refusals: new RefusalSink(),
    observer: tracing.sendObserver,
    suppression,
    send: {
      contracts: contractLookupOf(listed),
      resendOnTransientError: resendOnTransientError(config),
      core,
      observer: tracing.sendObserver,
      suppression,
      pause: async (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds)),
      now: () => Temporal.Now.instant(),
    },
    implementations: new Map(),
    workflows,
    telemetry: { handlers: () => registeredHandlers(context), tracing: tracing.setting },
  };

  recordTracingParts(context, tracing);

  return context;
}

/** The facts of the app's workflows that the core operations on a main thread read. */
export type WorkflowsOnMainThread = {
  readonly registry: WorkflowRegistry;
  readonly queues: QueueSetup;
  readonly resendOnTransientError: boolean;
};

/**
 * The core of an app on a main thread: one core handle of the app's manifest, and the operations of its sends, reads
 * and workflows on it, with its send rules. The app's child spec source is registered on that handle. Builds the
 * handle and registers the source; starts nothing.
 */
function addonCoreOf(
  accepted: AcceptedConfig,
  options: ReadonlyMap<string, NativeTaskOptions>,
  workflows: WorkflowsOnMainThread,
): AddonCore {
  const handle = buildCoreHandle(accepted.manifestJson, "PRODUCER").withInstallCoverage(accepted.installCoverage);

  const producer: ProducerCore = {
    prepareSend: async (taskName, facts) => prepareSendFacts(accepted.rules, facts, optionsOf(options, taskName)),
    enqueue: async (request, sendKind, trace) => enqueue(handle, request, sendKind, trace),
    getResult: async (taskId, wait) => getResult(handle, taskId, wait),
    taskInfo: async (taskId) => taskInfo(handle, taskId),
    processRole: async () => processRole(),
    workflows: mainThreadWorkflowCore(
      {
        handle,
        rules: accepted.rules,
        options,
        queueNames: admittedQueueNames(workflows.queues),
        resendOnTransientError: workflows.resendOnTransientError,
      },
      workflows.registry,
    ),
  };

  return Object.freeze({ producer: Object.freeze(producer), handle });
}

function optionsOf(options: ReadonlyMap<string, NativeTaskOptions>, taskName: string): NativeTaskOptions {
  const found = options.get(taskName);

  if (found === undefined) {
    throw new ClientDefect({ subject: "prepareSend", rule: `the task ${taskName} has no checked options` });
  }

  return found;
}
