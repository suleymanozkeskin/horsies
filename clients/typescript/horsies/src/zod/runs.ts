// The zod runs of the kind tree's hook nodes: a `checked` node runs zod checks, a `conversion` node runs a zod
// codec's functions. Each run goes through zod's own parse (`zod/v4/core`), so zod decides the order, the `when` and
// `abort` rules of its checks, and the issues.
//
// A check runs only the checks of its own node: the kinds below it are walked by the codec, so a container's check
// sees the value that the user holds (codec outputs, defaults filled), as zod's checks see it in both directions.

import * as core from "zod/v4/core";

import type {
  CheckFailure,
  CheckIssue,
  DecodeConversion,
  EncodeConversion,
  ValueCheck,
} from "../codec/conversion.ts";
import { entryPath, itemPath, memberPath, ROOT_PATH } from "../codec/refusal.ts";
import { isNumber, isPlainObject } from "../codec/values.ts";
import { err, ok, type Result } from "../wire/result.ts";
import type { ZodCheck, ZodCodecFunction, ZodSchema } from "./definition.ts";

/** The issue code of a check or a conversion that returned a promise: the codec runs them synchronously. */
export const ASYNC_ISSUE_CODE = "async_check";

/** The issue code of a one-way transform that zod met in the encode direction. */
export const ONE_WAY_ISSUE_CODE = "one_way_transform";

/** The issue code of a key of a record that its enum key does not hold. */
export const MISSING_KEY_ISSUE_CODE = "missing_key";

/** The definition type of a schema that accepts every value. */
const ANY_VALUE = "unknown";

/**
 * A check that runs `checks` on a value with zod's own check loop. Pure apart from the user's check functions.
 *
 * Fails with the issues of the checks (`CheckFailure`). A check that returns a promise is the issue
 * `ASYNC_ISSUE_CODE`. A user function that throws is not caught: it is a defect of that function.
 */
export function checksRun(checks: readonly ZodCheck[]): ValueCheck {
  const checkOnly = new core.$ZodUnknown({ type: ANY_VALUE, checks: [...checks] });

  return (value) => accepted(guarded(() => core.safeParse(checkOnly, value)));
}

/** A check that runs the whole parse of a leaf schema (a template literal: its pattern is its parse). */
export function parseRun(schema: ZodSchema): ValueCheck {
  return (value) => accepted(guarded(() => core.safeParse(schema, value)));
}

/** The two directions of a zod codec. */
export type CodecRuns = { readonly decode: DecodeConversion; readonly encode: EncodeConversion };

/**
 * The runs of a zod codec whose input side the codec walks itself (its stored kind): decode runs the codec's decode
 * and then the output schema; encode runs the output schema backward and then the codec's encode. Failures as
 * `checksRun`; a one-way transform in the output schema is the issue `ONE_WAY_ISSUE_CODE`.
 */
export function codecRuns(output: ZodSchema, decode: ZodCodecFunction, encode: ZodCodecFunction): CodecRuns {
  const passThrough = new core.$ZodCodec({
    type: "pipe",
    in: new core.$ZodUnknown({ type: ANY_VALUE }),
    out: output,
    transform: decode,
    reverseTransform: encode,
  });

  return {
    decode: (stored) => guarded(() => core.safeDecode(passThrough, stored)),
    encode: (value) => guarded(() => core.safeEncode(passThrough, value)),
  };
}

/**
 * The value of `value` decoded by a schema, as zod decodes it (a `.prefault()` value, which is an input value).
 * Failures as `codecRuns`.
 */
export function decodeRun(schema: ZodSchema): DecodeConversion {
  return (value) => guarded(() => core.safeDecode(schema, value));
}

/**
 * A check of the keys of a record: each key passes the key schema, and the record has each of `requiredKeys` (the
 * members of an enum key that `z.record()` requires). A value that is not a plain object passes: the writer refuses
 * it. Failures as `checksRun`: a missing key is the issue `MISSING_KEY_ISSUE_CODE` at the key's declared name; a key
 * that the key schema refuses has the key schema's issue codes at `<input>`, since the key is part of the value.
 */
export function recordKeysRun(keySchema: ZodSchema, requiredKeys: readonly string[]): ValueCheck {
  const keyCheck = parseRun(keySchema);

  return (value) => {
    if (!isPlainObject(value)) {
      return ok(true);
    }

    const keys = Object.keys(value);

    const missing = requiredKeys
      .filter((key) => !keys.includes(key))
      .map((key) => ({ path: memberPath(ROOT_PATH, key), code: MISSING_KEY_ISSUE_CODE }));

    const refusedKeys = keys.flatMap((key) => keyIssues(keyCheck(key)));
    const issues = [...missing, ...refusedKeys];

    return issues.length === 0 ? ok(true) : err(Object.freeze({ issues: Object.freeze(issues) }));
  };
}

function keyIssues(checked: Result<true, CheckFailure>): readonly CheckIssue[] {
  return checked.ok ? [] : checked.error.issues.map((issue) => ({ path: entryPath(ROOT_PATH), code: issue.code }));
}

type ZodOutcome = core.util.SafeParseResult<unknown>;

function accepted(outcome: Result<unknown, CheckFailure>): Result<true, CheckFailure> {
  return outcome.ok ? ok(true) : outcome;
}

/**
 * The outcome of one zod run. zod throws for an async check or conversion in a synchronous run, and for a one-way
 * transform in the encode direction: both are named issues. Any other throw is not zod's: it passes on.
 */
function guarded(run: () => ZodOutcome): Result<unknown, CheckFailure> {
  try {
    return outcomeOf(run());
  } catch (error) {
    if (error instanceof core.$ZodAsyncError) {
      return err(oneIssue(ASYNC_ISSUE_CODE));
    }

    if (error instanceof core.$ZodEncodeError) {
      return err(oneIssue(ONE_WAY_ISSUE_CODE));
    }

    throw error;
  }
}

function outcomeOf(outcome: ZodOutcome): Result<unknown, CheckFailure> {
  if (outcome.success) {
    return ok(outcome.data);
  }

  const issues = outcome.error.issues.map((issue) => Object.freeze({ path: pathText(issue.path), code: issue.code }));

  return err(Object.freeze({ issues: Object.freeze(issues) }));
}

function oneIssue(code: string): CheckFailure {
  return Object.freeze({ issues: Object.freeze([Object.freeze({ path: ROOT_PATH, code })]) });
}

/**
 * The path of a zod issue inside the value: `[n]` for an index, `<input>` for a key. A key in a zod issue path can be
 * a record key, which is part of the value, so no key is written (`CheckIssue`). Pure.
 */
function pathText(path: readonly PropertyKey[]): string {
  return path.reduce<string>((text, part) => (isNumber(part) ? itemPath(text, part) : entryPath(text)), ROOT_PATH);
}
