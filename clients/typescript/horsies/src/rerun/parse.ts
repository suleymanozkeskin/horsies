// The parse of the command and the policy of a rerun into the records of the addon, before any call.
// The bounds come from the core's generated constants; the class key of the retention is the core's.

import { RESERVATION_WINDOW_MICROS_MAX, RESERVATION_WINDOW_MICROS_MIN } from "../_catalog/manifest.ts";
import { RETENTION_DAYS_MAX, RETENTION_DAYS_MIN } from "../_catalog/worker_manifest.ts";
import type {
  CallerKeyArg,
  DeadlineArg,
  RerunCommandArg,
  RerunPolicyArg,
  RetentionArg,
} from "../native/facades/rerun.ts";
import type { Result } from "../wire/result.ts";
import { err, ok } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import { invalidCommand, invalidPolicy } from "./errors.ts";
import type { RerunInvalidCommand, RerunInvalidPolicy, RerunRequestRefused } from "./errors.ts";
import type { RerunCommand, RerunDeadline, RerunPolicy, RerunRetention } from "./types.ts";

/** The command and the policy of one rerun, as the addon takes them. */
export interface RerunRequest {
  readonly command: RerunCommandArg;
  readonly policy: RerunPolicyArg;
}

/** 0001-01-01T00:00:00Z in epoch microseconds: the earliest instant of Python's `datetime`. */
const DEADLINE_EARLIEST_MICROS = -62_135_596_800_000_000n;

/** 9999-12-31T23:59:59.999999Z in epoch microseconds: the latest instant of Python's `datetime`. */
const DEADLINE_LATEST_MICROS = 253_402_300_799_999_999n;

const NANOSECONDS_PER_MICROSECOND = 1000n;
const NANOSECONDS_PER_MILLISECOND = 1_000_000n;
const NANOSECONDS_PER_SECOND = 1_000_000_000n;
const NANOSECONDS_PER_MINUTE = 60_000_000_000n;
const NANOSECONDS_PER_HOUR = 3_600_000_000_000n;
const NANOSECONDS_PER_DAY = 86_400_000_000_000n;

const CALLER_KEY_RULE = "callerKey must be well-formed text: it holds a lone UTF-16 surrogate";

const DEADLINE_RULE = "deadline.at must be from 0001-01-01T00:00:00Z to 9999-12-31T23:59:59.999999Z";

const RETENTION_RULE = `retention.days must be a whole number from ${RETENTION_DAYS_MIN} to ${RETENTION_DAYS_MAX}`;

const WINDOW_UNIT_RULE =
  "reservationWindow must use days and smaller units: a year, a month or a week has no fixed length";

const WINDOW_MICROSECOND_RULE = "reservationWindow must be a whole number of microseconds";

const WINDOW_RANGE_RULE = `reservationWindow must be more than 0 and at most 30 days (${RESERVATION_WINDOW_MICROS_MAX} microseconds)`;

/** The records of `command` and `policy`, or the refusal of the first field that breaks its rule. Pure. */
export function parseRerunRequest(command: RerunCommand, policy: RerunPolicy): Result<RerunRequest, RerunRequestRefused> {
  const commandArg = parseCommand(command);

  if (!commandArg.ok) {
    return commandArg;
  }

  const policyArg = parsePolicy(policy);

  if (!policyArg.ok) {
    return policyArg;
  }

  return ok({ command: commandArg.value, policy: policyArg.value });
}

/** The record of `command`. Pure. */
function parseCommand(command: RerunCommand): Result<RerunCommandArg, RerunInvalidCommand> {
  const deadline = parseDeadline(command.deadline);

  if (!deadline.ok) {
    return deadline;
  }

  const callerKey = parseCallerKey(command.callerKey);

  if (!callerKey.ok) {
    return callerKey;
  }

  return ok({ sourceTaskId: command.sourceTaskId, deadline: deadline.value, callerKey: callerKey.value });
}

/** The record of `deadline`: the instant cut toward the past to the microsecond. Pure. */
function parseDeadline(deadline: RerunDeadline): Result<DeadlineArg, RerunInvalidCommand> {
  switch (deadline.kind) {
    case "none":
      return ok({ kind: "noDeadline" });
    case "until": {
      const epochMicros = floorDivide(deadline.at.epochNanoseconds, NANOSECONDS_PER_MICROSECOND);

      return epochMicros >= DEADLINE_EARLIEST_MICROS && epochMicros <= DEADLINE_LATEST_MICROS
        ? ok({ kind: "until", epochMicros })
        : err(invalidCommand(DEADLINE_RULE));
    }

    default:
      return unreachable(deadline, "rerun deadline");
  }
}

/** The record of a caller key; an omitted key is an unkeyed rerun. Pure. */
function parseCallerKey(callerKey: string | undefined): Result<CallerKeyArg, RerunInvalidCommand> {
  if (callerKey === undefined) {
    return ok({ kind: "unkeyed" });
  }

  return callerKey.isWellFormed() ? ok({ kind: "keyed", key: callerKey }) : err(invalidCommand(CALLER_KEY_RULE));
}

/** The record of `policy`. Pure. */
function parsePolicy(policy: RerunPolicy): Result<RerunPolicyArg, RerunInvalidPolicy> {
  const retention = parseRetention(policy.retention);

  if (!retention.ok) {
    return retention;
  }

  const windowMicros = parseWindow(policy.reservationWindow);

  if (!windowMicros.ok) {
    return windowMicros;
  }

  return ok({
    retention: retention.value,
    rerunInput: policy.rerunInput,
    reservationWindowMicros: windowMicros.value,
  });
}

/** The record of `retention`. Pure. */
function parseRetention(retention: RerunRetention): Result<RetentionArg, RerunInvalidPolicy> {
  switch (retention.kind) {
    case "forever":
      return ok({ kind: "forever" });
    case "days":
      return Number.isInteger(retention.days) && retention.days >= RETENTION_DAYS_MIN && retention.days <= RETENTION_DAYS_MAX
        ? ok({ kind: "days", days: retention.days })
        : err(invalidPolicy(RETENTION_RULE));
    default:
      return unreachable(retention, "rerun retention");
  }
}

/** The microseconds of a reservation window. Pure. */
function parseWindow(window: Temporal.Duration): Result<bigint, RerunInvalidPolicy> {
  if (window.years !== 0 || window.months !== 0 || window.weeks !== 0) {
    return err(invalidPolicy(WINDOW_UNIT_RULE));
  }

  const nanoseconds =
    BigInt(window.days) * NANOSECONDS_PER_DAY +
    BigInt(window.hours) * NANOSECONDS_PER_HOUR +
    BigInt(window.minutes) * NANOSECONDS_PER_MINUTE +
    BigInt(window.seconds) * NANOSECONDS_PER_SECOND +
    BigInt(window.milliseconds) * NANOSECONDS_PER_MILLISECOND +
    BigInt(window.microseconds) * NANOSECONDS_PER_MICROSECOND +
    BigInt(window.nanoseconds);

  if (nanoseconds % NANOSECONDS_PER_MICROSECOND !== 0n) {
    return err(invalidPolicy(WINDOW_MICROSECOND_RULE));
  }

  const micros = nanoseconds / NANOSECONDS_PER_MICROSECOND;

  return micros >= BigInt(RESERVATION_WINDOW_MICROS_MIN) && micros <= BigInt(RESERVATION_WINDOW_MICROS_MAX)
    ? ok(micros)
    : err(invalidPolicy(WINDOW_RANGE_RULE));
}

/** `dividend` divided by `divisor`, rounded toward negative infinity. `divisor` is above 0. Pure. */
function floorDivide(dividend: bigint, divisor: bigint): bigint {
  const quotient = dividend / divisor;

  return dividend % divisor < 0n ? quotient - 1n : quotient;
}
