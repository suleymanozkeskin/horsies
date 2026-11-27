// The retention settings of the config: how long records stay, the history leaves kept ahead of writes, the periods
// that sends may choose, and the retention of each queue (the core's `RetentionConfig`,
// `core/src/core/config/retention.rs`). A refusal of a retention field has the code of the retention config
// (`HRS-216`).

import { HorsiesErrorCode } from "../_catalog/error_codes.ts";
import { PAUSED_AGE_MICROS_MAX, PAUSED_AGE_MICROS_MIN } from "../_catalog/worker_manifest.ts";
import { refusal } from "../app/refusal.ts";
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
  keyPath,
  mapChecked,
  memberPath,
  optionalEntry,
  refused,
  u32Field,
  u64Field,
} from "./checked.ts";

/** How long a kind of record stays, in hours, or no pruning. */
export type HoursRetention = { readonly kind: "hours"; readonly hours: number } | { readonly kind: "noPruning" };

/** The age past which a paused workflow is cancelled, or never. */
export type PausedWorkflowExpiry = { readonly kind: "never" } | { readonly kind: "after"; readonly age: Temporal.Duration };

/** A retention period of whole days. */
export type RetentionPeriod = { readonly days: number };

/** The retention of the history records of a queue: a period of whole days, or forever. */
export type QueueRetention = { readonly kind: "days"; readonly days: number } | { readonly kind: "forever" };

/** The retention settings. Each field left out has the core's default; the core checks each range. */
export interface RetentionConfigInput {
  /** Worker state snapshots. */
  readonly workerStateRetention?: HoursRetention;
  /** Terminal workflow records. */
  readonly terminalRecordRetention?: HoursRetention;
  readonly pausedWorkflowCancelAfter?: PausedWorkflowExpiry;
  readonly historyLeafHorizonDays?: number;
  readonly heartbeatLeafHorizonHours?: number;
  /** The periods that a send may choose besides the 30-day default and the periods of `queueRetention`. */
  readonly retentionPeriods?: readonly RetentionPeriod[];
  /** The retention of each named queue; a queue that is not named has the default. */
  readonly queueRetention?: Readonly<Record<string, QueueRetention>>;
  readonly partitionMaintenanceIntervalSeconds?: number;
  readonly retentionSweepIntervalSeconds?: number;
  readonly retentionDeleteBatchSize?: number;
}

const SECONDS_PER_DAY = 86_400;
const NANOSECONDS_PER_MICROSECOND = 1000n;
const NANOSECONDS_PER_SECOND = 1_000_000_000n;
const NANOSECONDS_PER_MILLISECOND = 1_000_000n;
const NANOSECONDS_PER_MINUTE = 60n * NANOSECONDS_PER_SECOND;
const NANOSECONDS_PER_HOUR = 60n * NANOSECONDS_PER_MINUTE;
const NANOSECONDS_PER_DAY = 24n * NANOSECONDS_PER_HOUR;
const DAYS_PER_WEEK = 7n;

/** A `u32` field of the retention config. */
const RETENTION_U32 = u32Field(HorsiesErrorCode.CONFIG_INVALID_RETENTION);

/** A `u64` field of the retention config. */
const RETENTION_U64 = u64Field(HorsiesErrorCode.CONFIG_INVALID_RETENTION);

/** A count of whole days whose seconds are a safe integer. */
const DAY_COUNT: IntegerBound = Object.freeze({
  min: 0,
  max: Math.floor(Number.MAX_SAFE_INTEGER / SECONDS_PER_DAY),
  code: HorsiesErrorCode.CONFIG_INVALID_RETENTION,
});

/** The core's `retention` object of `input`. Pure. */
export function retentionJson(input: RetentionConfigInput, path: string): Checked<ConfigJson> {
  const integer =
    (name: string, bound: IntegerBound) =>
    (value: number): Checked<ConfigJson> =>
      checkedInteger(value, bound, memberPath(path, name));

  return checkedObject([
    optionalEntry("worker_state_retention_hours", input.workerStateRetention, (value) =>
      hoursJson(value, memberPath(path, "workerStateRetention.hours")),
    ),
    optionalEntry("terminal_record_retention_hours", input.terminalRecordRetention, (value) =>
      hoursJson(value, memberPath(path, "terminalRecordRetention.hours")),
    ),
    optionalEntry("paused_workflow_auto_cancel_after", input.pausedWorkflowCancelAfter, (value) =>
      pausedExpiryJson(value, memberPath(path, "pausedWorkflowCancelAfter.age")),
    ),
    optionalEntry(
      "history_leaf_horizon_days",
      input.historyLeafHorizonDays,
      integer("historyLeafHorizonDays", RETENTION_U32),
    ),
    optionalEntry(
      "heartbeat_leaf_horizon_hours",
      input.heartbeatLeafHorizonHours,
      integer("heartbeatLeafHorizonHours", RETENTION_U32),
    ),
    optionalEntry("retention_periods", input.retentionPeriods, (periods) =>
      periodsJson(periods, memberPath(path, "retentionPeriods")),
    ),
    optionalEntry("queue_retention", input.queueRetention, (mapping) =>
      queueRetentionJson(mapping, memberPath(path, "queueRetention")),
    ),
    optionalEntry(
      "partition_maintenance_interval_s",
      input.partitionMaintenanceIntervalSeconds,
      integer("partitionMaintenanceIntervalSeconds", RETENTION_U64),
    ),
    optionalEntry(
      "retention_sweep_interval_s",
      input.retentionSweepIntervalSeconds,
      integer("retentionSweepIntervalSeconds", RETENTION_U64),
    ),
    optionalEntry(
      "retention_delete_batch_size",
      input.retentionDeleteBatchSize,
      integer("retentionDeleteBatchSize", RETENTION_U32),
    ),
  ]);
}

/** The core's form of an hours retention: the hours, or `null` for no pruning. Pure. */
function hoursJson(retention: HoursRetention, path: string): Checked<ConfigJson> {
  switch (retention.kind) {
    case "hours":
      return checkedInteger(retention.hours, RETENTION_U32, path);
    case "noPruning":
      return accepted(null);
    default:
      return unreachable(retention, "hoursJson");
  }
}

function periodsJson(periods: readonly RetentionPeriod[], path: string): Checked<ConfigJson> {
  return checkedArray(periods.map((period, index) => daysJson(period.days, memberPath(itemPath(path, index), "days"))));
}

/** The core's form of a period of whole days: the duration as `[seconds, nanoseconds]`. Pure. */
function daysJson(days: number, path: string): Checked<ConfigJson> {
  return mapChecked(checkedInteger(days, DAY_COUNT, path), (whole) => Object.freeze([whole * SECONDS_PER_DAY, 0]));
}

function queueRetentionJson(mapping: Readonly<Record<string, QueueRetention>>, path: string): Checked<ConfigJson> {
  return checkedObject(
    Object.entries(mapping).map(([queue, retention]) => {
      const at = keyPath(path, queue);
      const name = checkedText(queue, HorsiesErrorCode.CONFIG_INVALID_RETENTION, at);

      return name.kind === "refused" ? name : entry(queue, queueRetentionValue(retention, at));
    }),
  );
}

/** The core's form of a queue retention: the period, or `null` for forever. Pure. */
function queueRetentionValue(retention: QueueRetention, path: string): Checked<ConfigJson> {
  switch (retention.kind) {
    case "days":
      return daysJson(retention.days, memberPath(path, "days"));
    case "forever":
      return accepted(null);
    default:
      return unreachable(retention, "queueRetentionValue");
  }
}

function pausedExpiryJson(expiry: PausedWorkflowExpiry, path: string): Checked<ConfigJson> {
  switch (expiry.kind) {
    case "never":
      return accepted(null);
    case "after":
      return pausedAgeJson(expiry.age, path);
    default:
      return unreachable(expiry, "pausedExpiryJson");
  }
}

/**
 * The core's form of a paused age, `[seconds, nanoseconds]`, when the age is the core's `PausedWorkflowAge`: whole
 * microseconds from `PAUSED_AGE_MICROS_MIN` to `PAUSED_AGE_MICROS_MAX`, with no years or months (their length is not
 * fixed). Else a retention refusal (`HRS-216`). Pure.
 */
function pausedAgeJson(age: Temporal.Duration, path: string): Checked<ConfigJson> {
  const help = ["give a positive age in days, hours, minutes, seconds or smaller units, in whole microseconds"];

  if (age.years !== 0 || age.months !== 0) {
    return refused(refusal(HorsiesErrorCode.CONFIG_INVALID_RETENTION, path, "years and months have no fixed length", help));
  }

  const nanoseconds = durationNanoseconds(age);
  const micros = nanoseconds / NANOSECONDS_PER_MICROSECOND;

  if (
    nanoseconds % NANOSECONDS_PER_MICROSECOND !== 0n ||
    micros < PAUSED_AGE_MICROS_MIN ||
    micros > PAUSED_AGE_MICROS_MAX
  ) {
    return refused(
      refusal(
        HorsiesErrorCode.CONFIG_INVALID_RETENTION,
        path,
        `must be whole microseconds from ${PAUSED_AGE_MICROS_MIN} to ${PAUSED_AGE_MICROS_MAX}`,
        help,
      ),
    );
  }

  return accepted(
    Object.freeze([Number(nanoseconds / NANOSECONDS_PER_SECOND), Number(nanoseconds % NANOSECONDS_PER_SECOND)]),
  );
}

/** The length of a duration with no years or months, in nanoseconds: a week is 7 days, a day 24 hours. Pure. */
function durationNanoseconds(age: Temporal.Duration): bigint {
  return (
    BigInt(age.weeks) * DAYS_PER_WEEK * NANOSECONDS_PER_DAY +
    BigInt(age.days) * NANOSECONDS_PER_DAY +
    BigInt(age.hours) * NANOSECONDS_PER_HOUR +
    BigInt(age.minutes) * NANOSECONDS_PER_MINUTE +
    BigInt(age.seconds) * NANOSECONDS_PER_SECOND +
    BigInt(age.milliseconds) * NANOSECONDS_PER_MILLISECOND +
    BigInt(age.microseconds) * NANOSECONDS_PER_MICROSECOND +
    BigInt(age.nanoseconds)
  );
}
