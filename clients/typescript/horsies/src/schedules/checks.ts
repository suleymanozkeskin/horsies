// The checks of one schedule at `app.schedules`, in the order of the Python client; the first failing check of a
// schedule wins:
//
// 1. Every schedule, the checks of its fields (Python's `TaskSchedule` and `ScheduleConfig`): the pattern has a
//    stored text and, on a main thread, keeps the core's rules; `maxCatchUpRuns` is a whole number inside its bound;
//    no earlier schedule has its name.
// 2. An enabled schedule, the checks before a start (Python's `_prepare_schedule`): on a main thread, the core's zone
//    data holds the time zone; the contract is in the app's `tasks` list; the queue is a queue of the app; the input
//    encodes and reads back with the contract's input types; on a main thread, the input is at most the payload
//    reject bound (every slot sends the same input).
//
// The core's manifest parser checks the whole declaration again on a main thread. A check that needs the core does
// not run in a worker thread: no scheduler and no `check` runs there.

import { HorsiesErrorCode } from "../_catalog/error_codes.ts";
import { MAX_CATCH_UP_RUNS_MAX, MAX_CATCH_UP_RUNS_MIN } from "../_catalog/scheduler_manifest.ts";
import { refusal, type HorsiesError } from "../app/refusal.ts";
import { encodeParametersWithReadBack } from "../codec/read-back.ts";
import { refusalText } from "../codec/refusal.ts";
import type { QueueSetup } from "../config/queues.ts";
import { contractRecord } from "../contract/definition.ts";
import type { PatternCheck, RejectBound, TimezoneCheck } from "../native/scheduler/addon.ts";
import type { ContractLookup, ListedTask } from "../send/send.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import { DEFAULT_MAX_CATCH_UP_RUNS, DEFAULT_SCHEDULE_TIMEZONE, type DeclaredEntry } from "./entry.ts";
import { patternRefusalText, patternText } from "./pattern.ts";
import { checkedZone, type ZoneCheck } from "./zone.ts";

/** The core rules of a check on a main thread: the pattern and zone checks, and the payload bound. None elsewhere. */
export type CoreRules =
  | {
      readonly kind: "core";
      readonly checkPattern: (patternText: string) => PatternCheck;
      readonly checkTimezone: (name: string) => TimezoneCheck;
      readonly rejectBound: RejectBound;
    }
  | { readonly kind: "noCore" };

/** What the checks read from the app. */
export type CheckContext = { readonly contracts: ContractLookup; readonly queues: QueueSetup };

/** Whether an earlier schedule of the declaration has the name of a schedule. */
export type NameUse = { readonly kind: "firstUse" } | { readonly kind: "repeats"; readonly firstIndex: number };

/** The facts of each slot of an enabled schedule that passed every check. */
export type CheckedActivation = {
  readonly patternText: string;
  readonly timezone: string;
  readonly catchUpMissed: boolean;
  readonly maxCatchUpRuns: number;
  readonly task: ListedTask;
  readonly queueName: string;
  readonly inputJson: string;
};

/** A schedule that passed every check: disabled (its name only), or enabled with the facts of its slots. */
export type CheckedSchedule =
  | { readonly kind: "disabled"; readonly name: string }
  | { readonly kind: "enabled"; readonly name: string; readonly activation: CheckedActivation };

/** The fields of a schedule that passed the checks of every schedule. */
type CheckedFields = { readonly patternText: string; readonly maxCatchUpRuns: number };

/** The place of a schedule in a refusal: `schedules[2] daily-report`. Pure. */
export function schedulePath(index: number, name: string): string {
  return `schedules[${String(index)}] ${name}`;
}

/** The checked schedule of `entry` at `index`, or the refusal of its first failing check. */
export function checkSchedule(
  context: CheckContext,
  entry: DeclaredEntry,
  placement: { readonly index: number; readonly nameUse: NameUse },
  core: CoreRules,
): Result<CheckedSchedule, HorsiesError> {
  const path = schedulePath(placement.index, entry.name);
  const fields = checkedFields(entry, placement.nameUse, path, core);

  if (!fields.ok) {
    return fields;
  }

  if (entry.enabled === false) {
    return ok({ kind: "disabled", name: entry.name });
  }

  const activation = checkedActivation(context, entry, fields.value, path, core);

  return activation.ok ? ok({ kind: "enabled", name: entry.name, activation: activation.value }) : activation;
}

/** The checks of every schedule: the pattern, the catch-up count, the name. */
function checkedFields(entry: DeclaredEntry, nameUse: NameUse, path: string, core: CoreRules): Result<CheckedFields, HorsiesError> {
  const text = checkedPattern(entry, path, core);

  if (!text.ok) {
    return text;
  }

  const maxCatchUpRuns = boundedCount(entry.maxCatchUpRuns ?? DEFAULT_MAX_CATCH_UP_RUNS, MAX_CATCH_UP_RUNS_MIN, MAX_CATCH_UP_RUNS_MAX);

  if (!maxCatchUpRuns.ok) {
    return err(scheduleRefusal(path, `maxCatchUpRuns: ${maxCatchUpRuns.error.rule}`, maxCatchUpRuns.error.help));
  }

  switch (nameUse.kind) {
    case "firstUse":
      return ok({ patternText: text.value, maxCatchUpRuns: maxCatchUpRuns.value });
    case "repeats":
      return err(
        scheduleRefusal(path, `name: schedules[${String(nameUse.firstIndex)}] has the name "${entry.name}"`, "give each schedule its own name"),
      );
    default:
      return unreachable(nameUse, "checkedFields");
  }
}

/** Why a count is outside its bound: the rule, and the help line. */
export type CountOutside = { readonly rule: string; readonly help: string };

/** `value` when it is a whole number from `min` to `max`, else why it is outside. Pure. */
export function boundedCount(value: number, min: number, max: number): Result<number, CountOutside> {
  const bound = `a whole number from ${String(min)} to ${String(max)}`;

  return Number.isSafeInteger(value) && value >= min && value <= max
    ? ok(value)
    : err({ rule: `${String(value)} is not ${bound}`, help: `give ${bound}` });
}

/** The stored pattern text of `entry`, checked by the core's rules on a main thread. */
function checkedPattern(entry: DeclaredEntry, path: string, core: CoreRules): Result<string, HorsiesError> {
  const text = patternText(entry.pattern);

  if (!text.ok) {
    return err(scheduleRefusal(path, patternRefusalText(text.error), "write each number of the pattern as a whole number"));
  }

  switch (core.kind) {
    case "noCore":
      return text;
    case "core": {
      const checked = core.checkPattern(text.value);

      switch (checked.kind) {
        case "accepted":
          return text;
        case "refused":
          return err(scheduleRefusal(path, `pattern: ${checked.cause}`, "keep each field of the pattern inside its bound"));
        default:
          return unreachable(checked, "checkedPattern");
      }
    }

    default:
      return unreachable(core, "checkedPattern");
  }
}

/** The facts of the slots of an enabled schedule, or the refusal of its first failing check. */
function checkedActivation(
  context: CheckContext,
  entry: DeclaredEntry,
  fields: CheckedFields,
  path: string,
  core: CoreRules,
): Result<CheckedActivation, HorsiesError> {
  const timezone = checkedZone(entry.timezone ?? DEFAULT_SCHEDULE_TIMEZONE, zoneCheckOf(core), (rule) =>
    scheduleRefusal(path, rule.message, rule.help),
  );

  if (!timezone.ok) {
    return timezone;
  }

  const listed = context.contracts.byContract(entry.task);

  if (listed.kind === "notListed") {
    const taskName = contractRecord(entry.task).name;

    return err(
      refusal(HorsiesErrorCode.TASK_NOT_REGISTERED, path, `the task ${taskName} is not in the tasks list of this app`, [
        "add its contract to Horsies.create({ tasks })",
      ]),
    );
  }

  const queueName = scheduleQueue(entry.queue, listed.task, context.queues, path);

  if (!queueName.ok) {
    return queueName;
  }

  const inputJson = checkedInput(entry, listed.task, path, core);

  return inputJson.ok
    ? ok({
        patternText: fields.patternText,
        timezone: timezone.value,
        catchUpMissed: entry.catchUpMissed === true,
        maxCatchUpRuns: fields.maxCatchUpRuns,
        task: listed.task,
        queueName: queueName.value,
        inputJson: inputJson.value,
      })
    : inputJson;
}

/** The zone check of `core`: the core's zone data on a main thread, none elsewhere. Pure. */
function zoneCheckOf(core: CoreRules): ZoneCheck {
  switch (core.kind) {
    case "core":
      return { kind: "core", check: core.checkTimezone };
    case "noCore":
      return core;
    default:
      return unreachable(core, "zoneCheckOf");
  }
}

/** The queue of each slot: the schedule's queue, checked against the queue mode, else the task's queue. Pure. */
function scheduleQueue(queue: string | undefined, task: ListedTask, queues: QueueSetup, path: string): Result<string, HorsiesError> {
  if (queue === undefined) {
    return ok(task.route.queueName);
  }

  switch (queues.kind) {
    case "default":
      return err(
        scheduleRefusal(path, `queue: the schedule names the queue "${queue}", and the default queue mode takes no queue`, 'leave queue out, or set queueMode: "custom" with customQueues'),
      );
    case "custom":
      return queues.queues.has(queue)
        ? ok(queue)
        : err(scheduleRefusal(path, `queue: "${queue}" is not a custom queue`, "name one of the customQueues, or leave queue out for the queue of the task"));
    default:
      return unreachable(queues, "scheduleQueue");
  }
}

/** The input text of each slot, read back with the contract's input types and inside the reject bound. */
function checkedInput(entry: DeclaredEntry, task: ListedTask, path: string, core: CoreRules): Result<string, HorsiesError> {
  const send = task.compiled.send;

  if (send.kind === "workflowNodeOnly") {
    return err(scheduleRefusal(path, `the task takes an upstream result (${send.upstream.join(", ")}); only a workflow node can run it`, "schedule a task that takes no upstream result"));
  }

  const text = encodeParametersWithReadBack(send.parameters, entry.input);

  if (!text.ok) {
    return err(scheduleRefusal(path, `input does not fit the task: ${refusalText(text.error)}`, "write the input as the input types of the task take it"));
  }

  switch (core.kind) {
    case "noCore":
      return ok(text.value);
    case "core":
      return withinRejectBound(text.value, core.rejectBound, path);
    default:
      return unreachable(core, "checkedInput");
  }
}

/** `text` when its UTF-8 size is inside `bound`, else the refusal. Pure. */
function withinRejectBound(text: string, bound: RejectBound, path: string): Result<string, HorsiesError> {
  switch (bound.kind) {
    case "noBound":
      return ok(text);
    case "bytes": {
      const size = BigInt(Buffer.byteLength(text, "utf8"));

      return size <= bound.bytes
        ? ok(text)
        : err(
            scheduleRefusal(
              path,
              `input is ${String(size)} bytes, over payload.rejectBytes=${String(bound.bytes)}; every slot sends the same input, so every send is refused`,
              "make the input smaller, or raise payload.rejectBytes",
            ),
          );
    }

    default:
      return unreachable(bound, "withinRejectBound");
  }
}

/** A refusal of a schedule (`HRS-205`) at `path`, with one help line. Pure. */
export function scheduleRefusal(path: string, message: string, help: string): HorsiesError {
  return refusal(HorsiesErrorCode.CONFIG_INVALID_SCHEDULE, path, message, [help]);
}
