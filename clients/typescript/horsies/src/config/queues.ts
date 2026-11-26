// The queue mode of the config: one default queue, or the custom queues (the core's `QueueMode` and
// `CustomQueueConfig`, `core/src/core/config/queue.rs`). A refusal of a queue field has the code of the queue mode
// (`HRS-200`).

import { ErrorCode } from "../_catalog/error_codes.ts";
import { unreachable } from "../wire/unreachable.ts";
import {
  accepted,
  type Checked,
  checkedArray,
  checkedInteger,
  checkedObject,
  checkedText,
  type ConfigJson,
  entry,
  type IntegerBound,
  itemPath,
  type JsonEntry,
  mapChecked,
  memberPath,
  optionalEntry,
  u32Field,
} from "./checked.ts";

/** A cap on a number of tasks, or no cap. */
export type TaskCap = { readonly kind: "cap"; readonly tasks: number } | { readonly kind: "noCap" };

/**
 * One custom queue. `priority`: 1 runs first, 100 runs last; the send of a task on the queue carries it, so it is
 * required. `maxConcurrency`: the most tasks of the queue that run at once (a cap of 0 pauses the queue); left out,
 * the core's default applies.
 */
export interface CustomQueueInput {
  readonly name: string;
  readonly priority: number;
  readonly maxConcurrency?: TaskCap;
}

/** One default queue, or the custom queues: at least one. */
export type QueueConfigInput =
  | { readonly queueMode?: "default"; readonly customQueues?: never }
  | { readonly queueMode: "custom"; readonly customQueues: readonly [CustomQueueInput, ...CustomQueueInput[]] };

/** The most tasks of a custom queue that run at once: the queue's own cap, or the core's default. */
export type QueueConcurrency = TaskCap | { readonly kind: "coreDefault" };

/** A custom queue of a parsed config. */
export type CustomQueue = {
  readonly name: string;
  readonly priority: number;
  readonly maxConcurrency: QueueConcurrency;
};

/** The queues of a parsed config: one default queue, or the custom queues by name. */
export type QueueSetup =
  | { readonly kind: "default" }
  | { readonly kind: "custom"; readonly queues: ReadonlyMap<string, CustomQueue> };

/** A `u32` field of a custom queue. */
const QUEUE_U32 = u32Field(ErrorCode.CONFIG_INVALID_QUEUE_MODE);

/** The core's members `queue_mode` and `custom_queues` of `input`. Pure. */
export function queueEntries(input: QueueConfigInput): Checked<readonly JsonEntry[]> {
  switch (input.queueMode) {
    case undefined:
    case "default":
      return accepted([]);
    case "custom":
      return customEntries(input.customQueues);
    default:
      return unreachable(input, "queueEntries");
  }
}

function customEntries(queues: readonly CustomQueueInput[]): Checked<readonly JsonEntry[]> {
  const items = checkedArray(queues.map((queue, index) => customQueueJson(queue, itemPath("customQueues", index))));

  return mapChecked(
    items,
    (list): readonly JsonEntry[] => [
      ["queue_mode", "custom"],
      ["custom_queues", list],
    ],
  );
}

function customQueueJson(queue: CustomQueueInput, path: string): Checked<ConfigJson> {
  return checkedObject([
    entry("name", checkedText(queue.name, ErrorCode.CONFIG_INVALID_QUEUE_MODE, memberPath(path, "name"))),
    entry("priority", checkedInteger(queue.priority, QUEUE_U32, memberPath(path, "priority"))),
    optionalEntry("max_concurrency", queue.maxConcurrency, (cap) =>
      taskCapJson(cap, QUEUE_U32, memberPath(path, "maxConcurrency.tasks")),
    ),
  ]);
}

/** The core's form of a cap: the number inside `bound`, or `null` for no cap. Pure. */
export function taskCapJson(cap: TaskCap, bound: IntegerBound, path: string): Checked<ConfigJson> {
  switch (cap.kind) {
    case "cap":
      return checkedInteger(cap.tasks, bound, path);
    case "noCap":
      return accepted(null);
    default:
      return unreachable(cap, "taskCapJson");
  }
}

/** The queues of an accepted input. Pure. */
export function queueSetupOf(input: QueueConfigInput): QueueSetup {
  switch (input.queueMode) {
    case undefined:
    case "default":
      return Object.freeze({ kind: "default" });
    case "custom":
      return Object.freeze({
        kind: "custom",
        queues: new Map(input.customQueues.map((queue) => [queue.name, customQueueOf(queue)])),
      });
    default:
      return unreachable(input, "queueSetupOf");
  }
}

function customQueueOf(queue: CustomQueueInput): CustomQueue {
  return Object.freeze({
    name: queue.name,
    priority: queue.priority,
    maxConcurrency: Object.freeze(queue.maxConcurrency ?? { kind: "coreDefault" }),
  });
}
