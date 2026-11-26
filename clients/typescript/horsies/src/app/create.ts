// `Horsies.create`: the checks of the config and of each listed contract, in order, and the app context.
//
// Order of the refusals: the config (the TypeScript parse, then the core's check), then the contracts in list order
// (the list, the queue, the wire types, then the core's checks of the codes and the options).
//
// The thread rule: the addon loads only on the main thread of a process. On a main thread every check runs; the core's
// checks call the addon's pure check functions, which start no core. On a worker thread only the checks that are
// TypeScript code run, and no addon loads.

import { isMainThread } from "node:worker_threads";

import { type AppConfig, type AppConfigInput, parseAppConfig, resendOnTransientErr } from "../config/app-config.ts";
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
import { ClientDefect } from "../thrown-errors.ts";
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
import type { CoreAccess, ProducerCore } from "./core-access.ts";
import { NO_OP_OBSERVER } from "./observers.ts";
import type { Refusal } from "./refusal.ts";
import { RefusalSink } from "./refusals.ts";
import { initialSuppression, type ProcessStart, processStart, SuppressionState } from "./suppression.ts";

/** The config of `Horsies.create`: the app config and the contract list. */
export type HorsiesConfig = AppConfigInput & {
  /** Every task contract of the app. A send of a contract that is not listed is refused. */
  readonly tasks: readonly AnyTaskContract[];
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
  /** The core operations of the app's sends and reads, on a main thread. */
  readonly producer: (accepted: AcceptedConfig, options: ReadonlyMap<string, NativeTaskOptions>) => ProducerCore;
}

/** The facts of this process. Effects: reads the thread, the environment and the arguments. */
export function processEnvironment(): CreateEnvironment {
  return {
    mainThread: isMainThread,
    environment: (name) => process.env[name],
    processStart: processStart(),
    coreChecks: () => ({ checkAppConfig, parseDeclaredErrorCodes, checkTaskOptions }),
    producer: addonProducer,
  };
}

/** The app context of `config`, or every refusal in order. */
export function createApp(config: HorsiesConfig, environment: CreateEnvironment): Result<AppContext, CreateRefused> {
  const parsed = parseAppConfig(config, environment.environment);
  // The queue check reads only the queue fields of the input, so a refused config adds no false queue refusal.
  const list = checkContractList(config.tasks, queueSetupOf(config));

  if (!parsed.ok) {
    return refused([...parsed.error, ...inListOrder(list.refusals)]);
  }

  if (!environment.mainThread) {
    return list.refusals.length > 0
      ? refused(inListOrder(list.refusals))
      : ok(contextOf(parsed.value, list.entries, { kind: "noCore" }, environment));
  }

  const checks = environment.coreChecks();
  const accepted = checkConfigWithCore(checks, parsed.value.coreConfigJson);
  const options = coreCheckedOptions(checks, list.entries);

  const refusals = [
    ...(accepted.kind === "refused" ? accepted.refusals : []),
    ...inListOrder([...list.refusals, ...options.refusals]),
  ];

  if (accepted.kind === "refused" || refusals.length > 0) {
    return refused(refusals);
  }

  const core: CoreAccess = { kind: "addon", producer: environment.producer(accepted.value, options.options) };

  return ok(contextOf(parsed.value, list.entries, core, environment));
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
): AppContext {
  const suppression = new SuppressionState(initialSuppression(environment.processStart), environment.environment);

  return {
    config,
    listed,
    core,
    refusals: new RefusalSink(),
    observer: NO_OP_OBSERVER,
    suppression,
    send: {
      contracts: contractLookupOf(listed),
      resendOnTransientErr: resendOnTransientErr(config),
      core,
      observer: NO_OP_OBSERVER,
      suppression,
      pause: async (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds)),
      now: () => Temporal.Now.instant(),
    },
    implementations: new Map(),
  };
}

/** The core operations of an app on a main thread: a core handle of the app's manifest, and its send rules. */
function addonProducer(accepted: AcceptedConfig, options: ReadonlyMap<string, NativeTaskOptions>): ProducerCore {
  const handle = buildCoreHandle(accepted.manifestJson, "PRODUCER").withInstallCoverage(accepted.installCoverage);

  const producer: ProducerCore = {
    prepareSend: async (facts) => prepareSendFacts(accepted.rules, facts, optionsOf(options, facts.taskName)),
    enqueue: async (request, sendKind, trace) => enqueue(handle, request, sendKind, trace),
    getResult: async (taskId, wait) => getResult(handle, taskId, wait),
    taskInfo: async (taskId) => taskInfo(handle, taskId),
    processRole: async () => processRole(),
  };

  return Object.freeze(producer);
}

function optionsOf(options: ReadonlyMap<string, NativeTaskOptions>, taskName: string): NativeTaskOptions {
  const found = options.get(taskName);

  if (found === undefined) {
    throw new ClientDefect({ subject: "prepareSend", rule: `the task ${taskName} has no checked options` });
  }

  return found;
}
