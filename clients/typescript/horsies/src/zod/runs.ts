// The zod runs of the kind tree's hook nodes: a `checked` node runs zod checks, a `conversion` node runs a zod
// codec's functions.
//
// Each run uses only the user's own schema objects, so a schema of each zod copy runs (the ES module or the CommonJS
// build of an install, a copy in a bundle, another install of a supported release): a check runs through its own
// `_zod.check`, a codec through its own decode and encode functions, and a schema through its own `_zod.run` (the
// function that zod's `safeParse` and `safeEncode` call). This module builds no zod node and imports no zod module.
//
// A check runs only the checks of its own node: the kinds below it are walked by the codec. As in zod, a check sees a
// value of its node's structure, with codec outputs and defaults filled: at encode the writer writes the node's kind
// first and gives the check the sent value with its defaults filled (`codec/check-view.ts`); at decode the reader
// gives the worker-form value.

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
import type { ZodCheck, ZodCodecFunction, ZodPayload, ZodRunContext, ZodSchema } from "./internals.ts";

/** The issue code of a check or a conversion that returned a promise: the codec runs them synchronously. */
export const ASYNC_ISSUE_CODE = "async_check";

/** The issue code of a one-way transform that zod met in the encode direction. */
export const ONE_WAY_ISSUE_CODE = "one_way_transform";

/** The issue code of a key of a record that its enum key does not hold. */
export const MISSING_KEY_ISSUE_CODE = "missing_key";

/**
 * The message of the error that zod 4.1.13 to 4.6.x throw when a synchronous run meets a promise (`$ZodAsyncError`).
 * Each zod copy throws its own class of this error, and the error has no name of its own, so this text is the mark
 * that the errors of all copies share.
 */
export const ZOD_ASYNC_MESSAGE = "Encountered Promise during synchronous parse. Use .parseAsync() instead.";

/** The name of the error that zod throws for a one-way transform in the encode direction (`$ZodEncodeError`). */
export const ZOD_ENCODE_ERROR_NAME = "ZodEncodeError";

/** A zod parse payload: the value, and the issues that the checks and the parses add to it. */
type Payload = ZodPayload;

/** The direction of a zod run: `forward` parses (decodes) a value, `backward` encodes it. */
type Direction = ZodRunContext["direction"];

/**
 * A check that runs `checks` on a value in their order, each through its own `_zod.check`, with the rules of zod's
 * check loop:
 * - A check without a `when` rule does not run after an issue that does not continue (an issue that a check pushes
 *   without `continue: true`).
 * - A check with a `when` rule (zod's length checks) runs when its rule gives `true`, also after such an issue, but
 *   not after an issue that stops all checks (`continue: false`, from `abort: true`). This is the loop of zod 4.4 and
 *   later; zod 4.1 to 4.3 also run it there. The value is refused in both.
 *
 * Fails with the issues of the checks (`CheckFailure`). A check that returns a promise stops the run with the issue
 * `ASYNC_ISSUE_CODE`, as zod's synchronous parse does; the promise is not waited for. A user function that throws is
 * not caught: it is a defect of that function. Pure apart from the user's check functions.
 */
export function checksRun(checks: readonly ZodCheck[]): ValueCheck {
  const ordered = Object.freeze([...checks]);

  return (value) => guarded(() => accepted(checksOutcome(ordered, { value, issues: [] })));
}

function checksOutcome(checks: readonly ZodCheck[], payload: Payload): Result<unknown, CheckFailure> {
  let stopped = false;

  for (const check of checks) {
    if (!runsNext(check, payload, stopped)) {
      continue;
    }

    const before = payload.issues.length;
    const returned = check._zod.check(payload);

    if (returned instanceof Promise) {
      return err(oneIssue(ASYNC_ISSUE_CODE));
    }

    stopped = stopped || stopsLaterChecks(payload, before);
  }

  return payloadOutcome(payload);
}

/** Whether zod's check loop runs `check` after the issues so far. Calls the check's `when` rule. */
function runsNext(check: ZodCheck, payload: Payload, stopped: boolean): boolean {
  const when = check._zod.def.when;

  return when === undefined ? !stopped : !stopsAllChecks(payload) && when(payload);
}

/** Whether an issue from index `from` on stops the checks without a `when` rule (zod's `util.aborted`). Pure. */
function stopsLaterChecks(payload: Payload, from: number): boolean {
  return payload.aborted === true || payload.issues.slice(from).some((issue) => issue.continue !== true);
}

/** Whether an issue stops every later check, also one with a `when` rule (zod's `util.explicitlyAborted`). Pure. */
function stopsAllChecks(payload: Payload): boolean {
  return payload.aborted === true || payload.issues.some((issue) => issue.continue === false);
}

/**
 * A check that runs the whole parse of a schema through its own `_zod.run`: the pattern test of a template literal
 * without its checks, or the parse of a record key. Failures as `checksRun`.
 */
export function parseRun(schema: ZodSchema): ValueCheck {
  return (value) => guarded(() => accepted(payloadOf(runOf(schema, { value, issues: [] }, "forward"))));
}

/** The two directions of a zod codec. */
export type CodecRuns = { readonly decode: DecodeConversion; readonly encode: EncodeConversion };

/**
 * The runs of a zod codec whose input side the codec walks itself (its stored kind). These are the steps of zod's
 * codec after its input side, which zod 4.1 to 4.6 share: decode runs the codec's decode and then the output schema;
 * encode runs the output schema backward and then the codec's encode. Each step runs only when the step before it
 * added no issue. Failures as `checksRun`; a one-way transform in the output schema is the issue
 * `ONE_WAY_ISSUE_CODE`.
 */
export function codecRuns(output: ZodSchema, decode: ZodCodecFunction, encode: ZodCodecFunction): CodecRuns {
  return {
    decode: (stored) => guarded(() => decodeSteps(output, decode, { value: stored, issues: [] })),
    encode: (value) => guarded(() => encodeSteps(output, encode, { value, issues: [] })),
  };
}

function decodeSteps(output: ZodSchema, decode: ZodCodecFunction, stored: Payload): Result<unknown, CheckFailure> {
  const decoded = decode(stored.value, stored);

  if (decoded instanceof Promise) {
    return err(oneIssue(ASYNC_ISSUE_CODE));
  }

  return stored.issues.length > 0
    ? payloadOutcome(stored)
    : payloadOf(runOf(output, { value: decoded, issues: [] }, "forward"));
}

function encodeSteps(output: ZodSchema, encode: ZodCodecFunction, sent: Payload): Result<unknown, CheckFailure> {
  const backward = runOf(output, sent, "backward");

  if (!backward.ok || backward.value.issues.length > 0) {
    return payloadOf(backward);
  }

  const encoded = encode(backward.value.value, backward.value);

  if (encoded instanceof Promise) {
    return err(oneIssue(ASYNC_ISSUE_CODE));
  }

  return backward.value.issues.length > 0 ? payloadOutcome(backward.value) : ok(encoded);
}

/**
 * The value of `value` decoded by a schema through its own `_zod.run`, as zod decodes it (a `.prefault()` value,
 * which is an input value). Failures as `codecRuns`.
 */
export function decodeRun(schema: ZodSchema): DecodeConversion {
  return (value) => guarded(() => payloadOf(runOf(schema, { value, issues: [] }, "forward")));
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

/**
 * One synchronous run of a schema through its own `_zod.run`, in a new parse context (zod keeps the state of one
 * parse on its context). A run that does not end synchronously (a promise) is the issue `ASYNC_ISSUE_CODE`; it is not
 * waited for. Effects: the user functions of the schema.
 */
function runOf(schema: ZodSchema, payload: Payload, direction: Direction): Result<Payload, CheckFailure> {
  const context: ZodRunContext = { async: false, direction };
  const result = schema._zod.run(payload, context);

  return result instanceof Promise ? err(oneIssue(ASYNC_ISSUE_CODE)) : ok(result);
}

function payloadOf(run: Result<Payload, CheckFailure>): Result<unknown, CheckFailure> {
  return run.ok ? payloadOutcome(run.value) : run;
}

/** The value of a payload with no issue, or its issues. Pure. */
function payloadOutcome(payload: Payload): Result<unknown, CheckFailure> {
  if (payload.issues.length === 0) {
    return ok(payload.value);
  }

  const issues = payload.issues.map((issue) =>
    Object.freeze({ path: pathText(issue.path ?? []), code: issue.code }),
  );

  return err(Object.freeze({ issues: Object.freeze(issues) }));
}

function accepted(outcome: Result<unknown, CheckFailure>): Result<true, CheckFailure> {
  return outcome.ok ? ok(true) : outcome;
}

/**
 * The outcome of one run. zod throws for a promise in a synchronous run, and for a one-way transform in the encode
 * direction: both are named issues. Each zod copy throws its own classes of these errors, so the adapter knows them by
 * their text (`ZOD_ASYNC_MESSAGE`, `ZOD_ENCODE_ERROR_NAME`). Any other throw is not zod's: it passes on.
 */
function guarded<T>(run: () => Result<T, CheckFailure>): Result<T, CheckFailure> {
  try {
    return run();
  } catch (error) {
    if (isZodAsyncError(error)) {
      return err(oneIssue(ASYNC_ISSUE_CODE));
    }

    if (isZodEncodeError(error)) {
      return err(oneIssue(ONE_WAY_ISSUE_CODE));
    }

    throw error;
  }
}

/** Whether a throw is zod's error for a promise in a synchronous run, from any zod copy. Pure. */
function isZodAsyncError(cause: unknown): boolean {
  return cause instanceof Error && cause.message === ZOD_ASYNC_MESSAGE;
}

/** Whether a throw is zod's error for a one-way transform in the encode direction, from any zod copy. Pure. */
function isZodEncodeError(cause: unknown): boolean {
  return cause instanceof Error && cause.name === ZOD_ENCODE_ERROR_NAME;
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
