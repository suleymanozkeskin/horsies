// The options of a send and their check. A delay is the only start option, as in both other clients.

import { WHOLE_SECONDS_MAX, WHOLE_SECONDS_MIN } from "../_catalog/manifest.ts";
import type { DeadlineFact, DelayFact, OptionalText, RetentionChoice } from "../native/sends/addon.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { instantText } from "../codec/temporal-text.ts";
import { IDEMPOTENCY_KEY_MAX_BYTES } from "./bounds.ts";

/** The retention of the task's history record: a period of whole days that the app registers, or forever. */
export type SendRetention = { readonly kind: "period"; readonly days: number } | { readonly kind: "forever" };

/** The options of `app.send`. */
export interface TaskSendOptions {
  /** A key of 1 to 255 UTF-8 bytes: a second send with the key in its reservation window gives the first task. */
  readonly idempotencyKey?: string;
  /** The task expires if no worker starts it before this instant. Cut to the microsecond, toward the past. */
  readonly goodUntil?: Temporal.Instant;
  /** Whole seconds before a worker may start the task: 0 to 4,294,967,295. */
  readonly delaySeconds?: number;
  /** Left out: the queue's retention, else the 30-day default. */
  readonly retention?: SendRetention;
}

/** The checked options of one send, as the core takes them. */
export type CheckedSendOptions = {
  readonly idempotencyKey: OptionalText;
  readonly deadline: DeadlineFact;
  readonly delay: DelayFact;
  readonly retention: RetentionChoice;
};

/** The send option that a check refuses, and the rule. `detail` holds no option value. */
export type SendOptionRefused = {
  readonly option: "idempotencyKey" | "goodUntil" | "delaySeconds" | "retention";
  readonly detail: string;
};

/** The highest day count of a retention period that the core reads (an unsigned 16-bit count). */
const RETENTION_DAYS_FIELD_MAX = 65_535;

const NANOSECONDS_PER_MICROSECOND = 1000n;

/** The checked options, or the first refusal in field order. Pure. */
export function checkSendOptions(options: TaskSendOptions): Result<CheckedSendOptions, SendOptionRefused> {
  const key = checkKey(options.idempotencyKey);
  const deadline = checkDeadline(options.goodUntil);
  const delay = checkDelay(options.delaySeconds);
  const retention = checkRetention(options.retention);

  if (!key.ok) {
    return key;
  }

  if (!deadline.ok) {
    return deadline;
  }

  if (!delay.ok) {
    return delay;
  }

  if (!retention.ok) {
    return retention;
  }

  return ok(
    Object.freeze({ idempotencyKey: key.value, deadline: deadline.value, delay: delay.value, retention: retention.value }),
  );
}

function checkKey(key: string | undefined): Result<OptionalText, SendOptionRefused> {
  if (key === undefined) {
    return ok({ kind: "none" });
  }

  const bytes = Buffer.byteLength(key, "utf8");

  if (!key.isWellFormed()) {
    return refuse("idempotencyKey", "the key holds a lone surrogate");
  }

  if (bytes === 0 || bytes > IDEMPOTENCY_KEY_MAX_BYTES) {
    return refuse("idempotencyKey", `a key has 1 to ${IDEMPOTENCY_KEY_MAX_BYTES} UTF-8 bytes`);
  }

  return ok({ kind: "text", text: key });
}

function checkDeadline(goodUntil: Temporal.Instant | undefined): Result<DeadlineFact, SendOptionRefused> {
  if (goodUntil === undefined) {
    return ok({ kind: "noDeadline" });
  }

  if (!(goodUntil instanceof Temporal.Instant)) {
    return refuse("goodUntil", "goodUntil is a Temporal.Instant");
  }

  if (!instantText(goodUntil).ok) {
    return refuse("goodUntil", "a deadline is an instant of the years 1 to 9999, the range of a stored date-time");
  }

  return ok({ kind: "until", epochMicros: epochMicrosTowardPast(goodUntil) });
}

/** The epoch microseconds of an instant; a part below the microsecond is cut toward the past. Pure. */
export function epochMicrosTowardPast(instant: Temporal.Instant): bigint {
  const nanoseconds = instant.epochNanoseconds;
  const truncated = nanoseconds / NANOSECONDS_PER_MICROSECOND;

  return nanoseconds % NANOSECONDS_PER_MICROSECOND < 0n ? truncated - 1n : truncated;
}

function checkDelay(seconds: number | undefined): Result<DelayFact, SendOptionRefused> {
  if (seconds === undefined) {
    return ok({ kind: "noDelay" });
  }

  if (!Number.isSafeInteger(seconds) || seconds < WHOLE_SECONDS_MIN || seconds > WHOLE_SECONDS_MAX) {
    return refuse("delaySeconds", `a delay is whole seconds from ${WHOLE_SECONDS_MIN} to ${WHOLE_SECONDS_MAX}`);
  }

  return ok({ kind: "seconds", seconds });
}

function checkRetention(retention: SendRetention | undefined): Result<RetentionChoice, SendOptionRefused> {
  if (retention === undefined) {
    return ok({ kind: "queueOrDefault" });
  }

  switch (retention.kind) {
    case "forever":
      return ok({ kind: "forever" });
    case "period":
      return Number.isSafeInteger(retention.days) && retention.days >= 1 && retention.days <= RETENTION_DAYS_FIELD_MAX
        ? ok({ kind: "days", days: retention.days })
        : refuse("retention", "a retention period is a whole number of days from 1");
    default: {
      const unexpected: never = retention;

      throw new TypeError(`checkSendOptions: a retention outside its type: ${String(unexpected)}`);
    }
  }
}

function refuse<T>(option: SendOptionRefused["option"], detail: string): Result<T, SendOptionRefused> {
  return err(Object.freeze({ option, detail }));
}
