// The contract list of an app (`Horsies.create({ tasks })`): each listed contract, its queue route and its compiled
// form. The checks here are TypeScript code, so they run on every thread.

import { ErrorCode } from "../_catalog/error_codes.ts";
import {
  compileContract,
  type CompiledContract,
  type ContractRefused,
  type ContractRule,
} from "../contract/compile.ts";
import { contractRecord, type ContractRecord } from "../contract/definition.ts";
import type { QueueSetup } from "../config/queues.ts";
import type { ContractLookup, ListedLookup, ListedTask, QueueRoute } from "../send/send.ts";
import { unreachable } from "../wire/unreachable.ts";
import { refusal, type Refusal, type RefusalCode } from "./refusal.ts";

/**
 * The one queue of the default queue mode. The Python and Rust clients hold the same value
 * (`clients/rust/horsies/src/registry.rs`, `DEFAULT_QUEUE`); issue #2425 moves it to the core.
 */
export const DEFAULT_QUEUE_ROUTE: QueueRoute = Object.freeze({ queueName: "default" });

const NOT_LISTED: ListedLookup = Object.freeze({ kind: "notListed" });

/** One contract that passed the checks of the list, before the core checks. `index` is its place in the list. */
export type ListedEntry = {
  readonly contract: object;
  readonly index: number;
  readonly path: string;
  readonly task: ListedTask;
};

/** A refusal of the contract at `index` of the list. */
export type ListRefusal = { readonly index: number; readonly refusal: Refusal };

/** The outcome of the list checks: every entry that passed, and every refusal in list order. */
export type ListChecked = { readonly entries: readonly ListedEntry[]; readonly refusals: readonly ListRefusal[] };

/** The place of a listed contract in a refusal: `tasks[2] charge_payment`. Pure. */
export function listPath(index: number, name: string): string {
  return `tasks[${index}] ${name}`;
}

/**
 * Check each listed contract, in list order: no name twice and no object twice (HRS-301); the queue against the
 * queue mode (HRS-200, HRS-103); the task name (HRS-102); the parameter names and the wire types (HRS-105). Effects:
 * compiles each contract.
 */
export function checkContractList(contracts: readonly object[], queues: QueueSetup): ListChecked {
  const names = new Map<string, object>();
  const entries: ListedEntry[] = [];
  const refusals: ListRefusal[] = [];

  contracts.forEach((contract, index) => {
    const record = contractRecord(contract);
    const path = listPath(index, record.name);
    const first = names.get(record.name);

    if (first !== undefined) {
      const detail = first === contract ? "the contract is listed twice" : "another contract has the same task name";

      refusals.push({
        index,
        refusal: refusal(ErrorCode.TASK_DUPLICATE_NAME, path, detail, ["list each task once, under one name"]),
      });

      return;
    }

    names.set(record.name, contract);

    const checked = checkEntry(contract, record, queues, path);

    if (checked.kind === "listed") {
      entries.push({ contract, index, path, task: checked.task });
    } else {
      refusals.push(...checked.refusals.map((refused) => ({ index, refusal: refused })));
    }
  });

  return { entries, refusals };
}

type EntryChecked =
  | { readonly kind: "listed"; readonly task: ListedTask }
  | { readonly kind: "refused"; readonly refusals: readonly Refusal[] };

function checkEntry(contract: object, record: ContractRecord, queues: QueueSetup, path: string): EntryChecked {
  const route = queueRoute(record, queues, path);
  const compiled = compileContract(contract);

  const refusals = [
    ...(route.ok ? [] : [route.error]),
    ...(compiled.ok ? [] : compiled.error.map((refused) => contractRefusal(refused, path))),
  ];

  if (!route.ok || !compiled.ok) {
    return { kind: "refused", refusals };
  }

  return { kind: "listed", task: listedTask(compiled.value, route.value) };
}

/** A contract refusal at the listed contract `path`, with the catalog code of its rule and its help. Pure. */
function contractRefusal(refused: ContractRefused, path: string): Refusal {
  return refusal(contractRuleCode(refused.rule), path, `${refused.path}: ${refused.detail} [${refused.rule}]`, refused.help);
}

/**
 * The catalog code of a contract rule: a task name that cannot be stored is an invalid task option (HRS-102, as the
 * Python client and the core's task registry give it); each other rule is a signature that has no stored form (HRS-105).
 */
function contractRuleCode(rule: ContractRule): RefusalCode {
  switch (rule) {
    case "TaskName":
      return ErrorCode.TASK_INVALID_OPTIONS;
    case "ParameterName":
    case "SchemaVendor":
    case "Zod3Schema":
    case "ZodVersionNotSupported":
    case "ZodSchema":
    case "WireKind":
      return ErrorCode.TASK_STRICT_SIGNATURE_VIOLATION;
    default:
      return unreachable(rule, "contractRuleCode");
  }
}

function listedTask(compiled: CompiledContract, route: QueueRoute): ListedTask {
  return Object.freeze({ compiled, route });
}

type RouteChecked = { readonly ok: true; readonly value: QueueRoute } | { readonly ok: false; readonly error: Refusal };

/** The queue route of a contract under the queue mode. Pure. */
function queueRoute(record: ContractRecord, queues: QueueSetup, path: string): RouteChecked {
  switch (queues.kind) {
    case "default":
      return record.queue.kind === "noQueue"
        ? { ok: true, value: DEFAULT_QUEUE_ROUTE }
        : {
            ok: false,
            error: refusal(
              ErrorCode.CONFIG_INVALID_QUEUE_MODE,
              path,
              `the task names the queue "${record.queue.name}", and the default queue mode takes no queue`,
              ["remove queue, or set queueMode: \"custom\" with customQueues"],
            ),
          };
    case "custom": {
      if (record.queue.kind === "noQueue") {
        return {
          ok: false,
          error: refusal(ErrorCode.CONFIG_INVALID_QUEUE_MODE, path, "the custom queue mode needs a queue on each task", [
            "set queue to one of the customQueues",
          ]),
        };
      }

      const queue = queues.queues.get(record.queue.name);

      return queue === undefined
        ? {
            ok: false,
            error: refusal(ErrorCode.TASK_INVALID_QUEUE, path, `the queue "${record.queue.name}" is not a custom queue`, [
              "add the queue to customQueues, or name one of them",
            ]),
          }
        : { ok: true, value: Object.freeze({ queueName: queue.name }) };
    }

    default: {
      const unexpected: never = queues;

      throw new TypeError(`queueRoute: a queue setup outside its type: ${String(unexpected)}`);
    }
  }
}

/** The lookup of the send path over the listed tasks. Pure. */
export function contractLookup(entries: readonly ListedEntry[]): ContractLookup {
  const byContract = new Map(entries.map((entry) => [entry.contract, entry.task]));
  const byName = new Map(entries.map((entry) => [entry.task.compiled.record.name, entry.task]));

  return Object.freeze({
    byContract: (contract: object): ListedLookup => found(byContract.get(contract)),
    byName: (taskName: string): ListedLookup => found(byName.get(taskName)),
  });
}

function found(task: ListedTask | undefined): ListedLookup {
  return task === undefined ? NOT_LISTED : Object.freeze({ kind: "listed", task });
}

/** The refusals in list order; for one contract, in their own order (a stable sort). Pure. */
export function inListOrder(refusals: readonly ListRefusal[]): readonly Refusal[] {
  return refusals.toSorted((first, second) => first.index - second.index).map((listed) => listed.refusal);
}
