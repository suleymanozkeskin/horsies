// The checks of one schedule at `app.schedules`. The first failing check of a schedule wins (the order of the Rust
// client's `schedule_refusal`, then the zone):
//
// 1. Every schedule: the pattern has a stored text; on a main thread, the core's rules of the pattern hold.
// 2. An enabled schedule: the contract is in the app's `tasks` list; the queue is a queue of the app; the input encodes
//    and reads back with the contract's input types; on a main thread, the input is at most the payload reject bound
//    (every slot sends the same input); the time zone is an IANA zone name of the runtime; `maxCatchUpRuns` is a
//    whole number.
//
// The bounds of `maxCatchUpRuns` and of the section, and a repeated name, are rules of the core's manifest parser,
// which the declaration runs on a main thread. A check that needs the core does not run in a worker thread: no
// scheduler and no `check` runs there.

import { ErrorCode } from "../_catalog/error_codes.ts";
import { refusal, type Refusal } from "../app/refusal.ts";
import { encodeParametersWithReadBack } from "../codec/read-back.ts";
import { refusalText } from "../codec/refusal.ts";
import type { QueueSetup } from "../config/queues.ts";
import { contractRecord } from "../contract/definition.ts";
import type { PatternCheck, RejectBound } from "../native/scheduler/addon.ts";
import type { ContractLookup, ListedTask } from "../send/send.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import { DEFAULT_MAX_CATCH_UP_RUNS, DEFAULT_SCHEDULE_TIMEZONE, type DeclaredEntry } from "./entry.ts";
import { patternRefusalText, patternText } from "./pattern.ts";

/** The core rules of a check on a main thread: the pattern check and the payload bound. None in a worker thread. */
export type CoreRules =
  | {
      readonly kind: "core";
      readonly checkPattern: (patternText: string) => PatternCheck;
      readonly rejectBound: RejectBound;
    }
  | { readonly kind: "noCore" };

/** What the checks read from the app. */
export type CheckContext = { readonly contracts: ContractLookup; readonly queues: QueueSetup };

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

/** The place of a schedule in a refusal: `schedules[2] daily-report`. Pure. */
export function schedulePath(index: number, name: string): string {
  return `schedules[${String(index)}] ${name}`;
}

/** The checked schedule of `entry` at `index`, or the refusal of its first failing check. */
export function checkSchedule(
  context: CheckContext,
  entry: DeclaredEntry,
  index: number,
  core: CoreRules,
): Result<CheckedSchedule, Refusal> {
  const path = schedulePath(index, entry.name);
  const text = checkedPattern(entry, path, core);

  if (!text.ok) {
    return text;
  }

  if (entry.enabled === false) {
    return ok({ kind: "disabled", name: entry.name });
  }

  const activation = checkedActivation(context, entry, text.value, path, core);

  return activation.ok ? ok({ kind: "enabled", name: entry.name, activation: activation.value }) : activation;
}

/** The stored pattern text of `entry`, checked by the core's rules on a main thread. */
function checkedPattern(entry: DeclaredEntry, path: string, core: CoreRules): Result<string, Refusal> {
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
  patternJson: string,
  path: string,
  core: CoreRules,
): Result<CheckedActivation, Refusal> {
  const listed = context.contracts.byContract(entry.task);

  if (listed.kind === "notListed") {
    const taskName = contractRecord(entry.task).name;

    return err(
      refusal(ErrorCode.TASK_NOT_REGISTERED, path, `the task ${taskName} is not in the tasks list of this app`, [
        "add its contract to Horsies.create({ tasks })",
      ]),
    );
  }

  const queueName = scheduleQueue(entry.queue, listed.task, context.queues, path);

  if (!queueName.ok) {
    return queueName;
  }

  const inputJson = checkedInput(entry, listed.task, path, core);

  if (!inputJson.ok) {
    return inputJson;
  }

  const timezone = checkedTimezone(entry.timezone ?? DEFAULT_SCHEDULE_TIMEZONE, path);

  if (!timezone.ok) {
    return timezone;
  }

  const maxCatchUpRuns = entry.maxCatchUpRuns ?? DEFAULT_MAX_CATCH_UP_RUNS;

  if (!Number.isSafeInteger(maxCatchUpRuns)) {
    return err(scheduleRefusal(path, `maxCatchUpRuns: ${String(maxCatchUpRuns)} is not a whole number`, "give a count from 1 to 10000"));
  }

  return ok({
    patternText: patternJson,
    timezone: timezone.value,
    catchUpMissed: entry.catchUpMissed === true,
    maxCatchUpRuns,
    task: listed.task,
    queueName: queueName.value,
    inputJson: inputJson.value,
  });
}

/** The queue of each slot: the schedule's queue, checked against the queue mode, else the task's queue. Pure. */
function scheduleQueue(queue: string | undefined, task: ListedTask, queues: QueueSetup, path: string): Result<string, Refusal> {
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
function checkedInput(entry: DeclaredEntry, task: ListedTask, path: string, core: CoreRules): Result<string, Refusal> {
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
function withinRejectBound(text: string, bound: RejectBound, path: string): Result<string, Refusal> {
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

/** How the runtime reads a time zone text. */
export type ZoneReading =
  /** An IANA zone name as the runtime's zone data writes it. */
  | { readonly kind: "zoneName" }
  /** A UTC offset (`+05:00`): the zone of a schedule comes from an IANA name. */
  | { readonly kind: "offset" }
  /** A zone name in another case than the zone data's (`europe/berlin`). */
  | { readonly kind: "otherSpelling"; readonly spelling: string }
  /** No zone of the runtime. */
  | { readonly kind: "unknown" };

/** The start of a UTC offset text. An IANA zone name starts with a letter. */
const OFFSET_SIGNS: readonly string[] = ["+", "-"];

/**
 * How the runtime reads `name`, by `Temporal`, whose zone data `Intl` holds. Pure: reads no clock. The core checks
 * the name again with its own zone data at the scheduler start.
 */
export function zoneReading(name: string): ZoneReading {
  if (OFFSET_SIGNS.some((sign) => name.startsWith(sign))) {
    return { kind: "offset" };
  }

  let zoneId: string;

  try {
    zoneId = Temporal.ZonedDateTime.from({ timeZone: name, year: 2000, month: 1, day: 1 }).timeZoneId;
  } catch (cause) {
    // `Temporal` refuses a zone that its data does not hold with a `RangeError`; any other throw passes on.
    if (cause instanceof RangeError) {
      return { kind: "unknown" };
    }

    throw cause;
  }

  return zoneId === name ? { kind: "zoneName" } : { kind: "otherSpelling", spelling: zoneId };
}

/** `name` when it is an IANA zone name of the runtime, else the refusal. Pure. */
function checkedTimezone(name: string, path: string): Result<string, Refusal> {
  const reading = zoneReading(name);
  const help = 'use an IANA zone name, for example "UTC" or "Europe/Berlin"';

  switch (reading.kind) {
    case "zoneName":
      return ok(name);
    case "offset":
      return err(scheduleRefusal(path, `timezone: "${name}" is a UTC offset, not a zone name`, help));
    case "otherSpelling":
      return err(scheduleRefusal(path, `timezone: the zone data writes "${name}" as "${reading.spelling}"`, `write "${reading.spelling}"`));
    case "unknown":
      return err(scheduleRefusal(path, `timezone: "${name}" is not a zone that this runtime knows`, help));
    default:
      return unreachable(reading, "checkedTimezone");
  }
}

/** A refusal of a schedule (`HRS-205`) at `path`, with one help line. Pure. */
export function scheduleRefusal(path: string, message: string, help: string): Refusal {
  return refusal(ErrorCode.CONFIG_INVALID_SCHEDULE, path, message, [help]);
}
