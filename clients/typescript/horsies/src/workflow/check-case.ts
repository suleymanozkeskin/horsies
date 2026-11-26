// The params that `check` gives a build for one check case. A check case holds every param of the workflow, as the
// check cases of the Python and Rust clients do: the params that a start gives, and for each `h.taskOutcome` param
// the outcome that an upstream node would give (`{ kind: "succeeded", value }` or `{ kind: "failed", error }`).
//
// - The plain params are encoded with their kinds, read back, and decoded in the worker form, as `buildParams` does
//   for a start.
// - The success value of an outcome is encoded with the upstream output kind, read back, and decoded in the worker
//   form, so the build gets the value that a child build gets from a stored result.
// - A failure is given to the build as the case holds it: a task error has no declared kind to decode it with.

import { encodeParametersWithReadBack } from "../codec/read-back.ts";
import { decodeParameters } from "../codec/reader.ts";
import { type CodecRefused, decodeRefused, memberPath, ROOT_PATH } from "../codec/refusal.ts";
import { isObjectLike, setOwn } from "../codec/values.ts";
import { type Member, objectKind, type ObjectKind, REQUIRED } from "../wire/kinds.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { isTaskOutcomeKind, upstreamValueKind } from "../wire/task-outcome.ts";

/** One param of a check case: left out (an optional param), or the value that the build gets. */
type CaseValue = { readonly kind: "leftOut" } | { readonly kind: "given"; readonly value: unknown };

const LEFT_OUT: CaseValue = Object.freeze({ kind: "leftOut" });

/** The text of a refusal of a case value that is not an outcome. */
const NOT_AN_OUTCOME = 'expected an upstream outcome: { kind: "succeeded", value } or { kind: "failed", error }';

/**
 * The params value that a build gets for the check case `given`, with the members in the declaration order of
 * `params`; or the first refusal: of the plain params, then of each outcome param in order (a required outcome param
 * that the case leaves out, a value that is not an outcome, a success value that does not encode). Pure.
 */
export function checkCaseParams(params: ObjectKind, given: object): Result<object, CodecRefused> {
  const outcomes = params.members.filter((member) => isTaskOutcomeKind(member.kind));
  const plain = objectKind(params.members.filter((member) => !isTaskOutcomeKind(member.kind)));
  const plainValues = plainParams(plain, given, new Set(outcomes.map((member) => member.name)));

  if (!plainValues.ok) {
    return plainValues;
  }

  const values: Record<string, unknown> = {};

  for (const member of params.members) {
    const value = isTaskOutcomeKind(member.kind)
      ? caseOutcome(member, given)
      : ok(plainValue(plainValues.value, member));

    if (!value.ok) {
      return value;
    }

    if (value.value.kind === "given") {
      setOwn(values, member.name, value.value.value);
    }
  }

  return ok(Object.freeze(values));
}

/** The decoded plain param `member`: left out when the decode gives no member (an optional param). Pure. */
function plainValue(decoded: object, member: Member): CaseValue {
  return Object.hasOwn(decoded, member.name)
    ? Object.freeze({ kind: "given", value: Reflect.get(decoded, member.name) })
    : LEFT_OUT;
}

/** The plain params of `given` (its members but those of `outcomeNames`), encoded, read back and decoded. Pure. */
function plainParams(
  plain: ObjectKind,
  given: object,
  outcomeNames: ReadonlySet<string>,
): Result<object, CodecRefused> {
  const plainGiven: Record<string, unknown> = {};

  for (const [name, value] of Object.entries(given)) {
    if (!outcomeNames.has(name)) {
      setOwn(plainGiven, name, value);
    }
  }

  const text = encodeParametersWithReadBack(plain, plainGiven);

  if (!text.ok) {
    return text;
  }

  const decoded = decodeParameters(plain, text.value, "worker");

  return decoded.ok ? ok(Object(decoded.value)) : decoded;
}

/** The outcome param `member` of the case `given`. Pure. */
function caseOutcome(member: Member, given: object): Result<CaseValue, CodecRefused> {
  const path = memberPath(ROOT_PATH, member.name);

  if (!Object.hasOwn(given, member.name)) {
    return member.presence.mode === "required"
      ? err(decodeRefused("MissingMember", path, "a required member is missing"))
      : ok(LEFT_OUT);
  }

  const value: unknown = Reflect.get(given, member.name);

  if (value === null && member.kind.node === "nullable") {
    return ok(Object.freeze({ kind: "given", value: null }));
  }

  if (!isObjectLike(value)) {
    return err(decodeRefused("WrongType", path, NOT_AN_OUTCOME));
  }

  const tag: unknown = Reflect.get(value, "kind");

  if (tag === "succeeded" && Object.hasOwn(value, "value")) {
    return succeededOutcome(member, value, path);
  }

  const error: unknown = Reflect.get(value, "error");

  return tag === "failed" && isObjectLike(error)
    ? ok(Object.freeze({ kind: "given", value: Object.freeze({ kind: "failed", error }) }))
    : err(decodeRefused("WrongType", path, NOT_AN_OUTCOME));
}

/**
 * The success outcome of the case outcome `succeeded` (`{ kind: "succeeded", value }`): its value encoded with the
 * upstream output kind, read back, and decoded in the worker form.
 */
function succeededOutcome(member: Member, succeeded: object, path: string): Result<CaseValue, CodecRefused> {
  const holder = objectKind([{ name: member.name, kind: upstreamValueKind(member.kind, path), presence: REQUIRED }]);
  const holding: Record<string, unknown> = {};

  setOwn(holding, member.name, Reflect.get(succeeded, "value"));

  const text = encodeParametersWithReadBack(holder, holding);

  if (!text.ok) {
    return text;
  }

  const decoded = decodeParameters(holder, text.value, "worker");

  if (!decoded.ok) {
    return decoded;
  }

  const outcome = Object.freeze({ kind: "succeeded", value: Reflect.get(Object(decoded.value), member.name) });

  return ok(Object.freeze({ kind: "given", value: outcome }));
}
