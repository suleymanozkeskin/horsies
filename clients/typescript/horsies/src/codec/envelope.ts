// The stored result envelope and the stored task error, with the rules of the core's `read_task_result_text`
// (`core/src/core/task/result.rs`) and `TaskError::from_stored_text` (`core/src/core/task/error.rs`). A seat isolate
// reads its own input and cannot call the core, so the client holds these rules too.
//
// The envelope: `{"__h_task_result__": true, "ok": <value>, "err": null}` or `{..., "ok": null, "err": <error>}`.
// The error: `exception`, `error_code`, `data`, `message`; a sub-workflow error adds `sub_workflow_id` and
// `sub_workflow_summary`.

import type {
  ExceptionRecord,
  LibraryTaskCode,
  SubWorkflowFailure,
  TaskError,
  TaskErrorCode,
} from "../wire/task-error.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import {
  BUILTIN_CODE_KEY,
  ERROR_DATA_DEPTH_LIMIT,
  LIBRARY_TASK_CODES,
  RESERVED_NON_TASK_CODES,
  TASK_RESULT_ENVELOPE_KEY,
} from "./bounds.ts";
import type { JsonText } from "./json-text.ts";
import { decodeJsonNode, type JsonValue } from "./json-value.ts";
import { isTreeObject, readTree, type TreeObject, type TreeValue } from "./number-text.ts";
import {
  type DecodeRefused,
  entryPath,
  type EnvelopeRefused,
  envelopeRefused,
  type EnvelopeRule,
  memberPath,
  ROOT_PATH,
} from "./refusal.ts";
import { readSubWorkflow } from "./sub-workflow.ts";
import { isString } from "./values.ts";

const OK_SLOT = "ok";
const ERR_SLOT = "err";
const ERROR_CODE_KEY = "error_code";
const EXCEPTION_KEY = "exception";
const MESSAGE_KEY = "message";
const DATA_KEY = "data";
const SUB_WORKFLOW_ID_KEY = "sub_workflow_id";
const SUB_WORKFLOW_SUMMARY_KEY = "sub_workflow_summary";
const EXCEPTION_KEYS = ["type", "module", "message", "repr", "traceback"] as const;

const LIBRARY_CODES: ReadonlySet<string> = new Set(LIBRARY_TASK_CODES);
const RESERVED_CODES: ReadonlySet<string> = new Set(RESERVED_NON_TASK_CODES);

/** The slots of a stored result envelope: the `ok` value as its tree, or the stored error. */
export type EnvelopeSlots =
  | { readonly slot: "ok"; readonly value: TreeValue }
  | { readonly slot: "err"; readonly error: TaskError };

function refuse(rule: EnvelopeRule, path: string, detail: string): Result<never, EnvelopeRefused> {
  return err(envelopeRefused(rule, path, detail));
}

/**
 * The slots of a stored result envelope (`node` at `path`). A non-null `err` is the error; otherwise `ok` is the
 * value, `null` included. Refused: not an object, the marker missing or not `true`, another key, a missing slot,
 * both slots set, and every rule of the stored error. Pure.
 */
export function readEnvelopeSlots(node: TreeValue, path: string): Result<EnvelopeSlots, DecodeRefused> {
  if (!isTreeObject(node)) {
    return refuse("NotAnObject", path, "the envelope is not an object");
  }

  if (node[TASK_RESULT_ENVELOPE_KEY] !== true) {
    return refuse("MarkerMissing", path, "the marker is missing or is not true");
  }

  for (const key of Object.keys(node)) {
    if (key !== TASK_RESULT_ENVELOPE_KEY && key !== OK_SLOT && key !== ERR_SLOT) {
      return refuse("ExtraKey", entryPath(path), "a key other than the marker, ok and err");
    }
  }

  const okSlot = node[OK_SLOT];
  const errSlot = node[ERR_SLOT];

  if (okSlot === undefined || errSlot === undefined) {
    return refuse("MissingSlot", path, "the ok or the err slot is missing");
  }

  if (errSlot === null) {
    return ok({ slot: "ok", value: okSlot });
  }

  if (okSlot !== null) {
    return refuse("BothSlotsSet", path, "both the ok and the err slot are set");
  }

  const error = readErrorNode(errSlot, memberPath(path, ERR_SLOT));

  return error.ok ? ok({ slot: "err", error: error.value }) : error;
}

/** A stored task error object (the error column of a workflow). Pure. */
export function readStoredError(text: JsonText): Result<TaskError, DecodeRefused> {
  return readErrorNode(readTree(text), ROOT_PATH);
}

/**
 * The class of a stored error (the core's `TaskErrorKind`): a plain error, or the failure of a child workflow, which
 * has `sub_workflow_id` and `sub_workflow_summary` next to the four keys of every error.
 */
type StoredErrorClass = "plain" | "subWorkflow";

function isErrorKey(key: string, errorClass: StoredErrorClass): boolean {
  switch (key) {
    case EXCEPTION_KEY:
    case ERROR_CODE_KEY:
    case DATA_KEY:
    case MESSAGE_KEY:
      return true;
    case SUB_WORKFLOW_ID_KEY:
    case SUB_WORKFLOW_SUMMARY_KEY:
      return hasSubWorkflowKeys(errorClass);
    default:
      return false;
  }
}

function hasSubWorkflowKeys(errorClass: StoredErrorClass): boolean {
  switch (errorClass) {
    case "plain":
      return false;
    case "subWorkflow":
      return true;
    default:
      return unreachable(errorClass, "hasSubWorkflowKeys");
  }
}

/** A stored task error. `exception`, `data` and `message` may be missing or `null`. Pure. */
export function readErrorNode(node: TreeValue, path: string): Result<TaskError, DecodeRefused> {
  if (!isTreeObject(node)) {
    return refuse("ErrorNotAnObject", path, "the error is not an object");
  }

  const errorClass: StoredErrorClass = Object.hasOwn(node, SUB_WORKFLOW_ID_KEY) ? "subWorkflow" : "plain";

  for (const key of Object.keys(node)) {
    if (!isErrorKey(key, errorClass)) {
      return refuse("ErrorUnknownKey", entryPath(path), "a key that a stored error does not have");
    }
  }

  const codeNode = node[ERROR_CODE_KEY];

  if (codeNode === undefined) {
    return refuse("ErrorMissingField", memberPath(path, ERROR_CODE_KEY), "the error has no code");
  }

  const code = readCode(codeNode, memberPath(path, ERROR_CODE_KEY));

  return code.ok ? readErrorFields(node, code.value, errorClass, path) : code;
}

/** The fields of a stored error after its code. */
function readErrorFields(
  node: TreeObject,
  code: TaskErrorCode,
  errorClass: StoredErrorClass,
  path: string,
): Result<TaskError, DecodeRefused> {
  const exception = readException(node[EXCEPTION_KEY] ?? null, memberPath(path, EXCEPTION_KEY));

  if (!exception.ok) {
    return exception;
  }

  const message = readMessage(node[MESSAGE_KEY] ?? null, memberPath(path, MESSAGE_KEY));

  if (!message.ok) {
    return message;
  }

  const data = readData(node[DATA_KEY] ?? null, memberPath(path, DATA_KEY));

  if (!data.ok) {
    return data;
  }

  const subWorkflow = subWorkflowOf(node, errorClass, path);

  if (!subWorkflow.ok) {
    return subWorkflow;
  }

  return ok(
    Object.freeze({
      code,
      message: message.value,
      data: data.value,
      exception: exception.value,
      subWorkflow: subWorkflow.value,
    }),
  );
}

/** The failed child workflow of a sub-workflow error; `null` for a plain error. */
function subWorkflowOf(
  node: TreeObject,
  errorClass: StoredErrorClass,
  path: string,
): Result<SubWorkflowFailure | null, DecodeRefused> {
  switch (errorClass) {
    case "plain":
      return ok(null);
    case "subWorkflow":
      return readSubWorkflow(node[SUB_WORKFLOW_ID_KEY], node[SUB_WORKFLOW_SUMMARY_KEY], path);
    default:
      return unreachable(errorClass, "subWorkflowOf");
  }
}

function isLibraryCode(text: string): text is LibraryTaskCode {
  return LIBRARY_CODES.has(text);
}

/** A user code text, or `{"__builtin_task_code__": "<library code>"}`, with exactly that one key. */
function readCode(node: TreeValue, path: string): Result<TaskErrorCode, EnvelopeRefused> {
  if (isString(node)) {
    return LIBRARY_CODES.has(node) || RESERVED_CODES.has(node)
      ? refuse("ErrorFieldInvalid", path, "a user code that names a library or reserved code")
      : ok(Object.freeze({ kind: "user", code: node }));
  }

  if (!isTreeObject(node)) {
    return refuse("ErrorFieldInvalid", path, "the code is not a string or a tagged object");
  }

  const tagged = node[BUILTIN_CODE_KEY];

  if (Object.keys(node).length !== 1 || !isString(tagged)) {
    return refuse("ErrorFieldInvalid", path, "the tagged code has another form");
  }

  return isLibraryCode(tagged)
    ? ok(Object.freeze({ kind: "library", code: tagged }))
    : refuse("ErrorUnknownBuiltInCode", path, "the tag names no library code");
}

/** The exception record: `null`, or an object of exactly the five text keys. */
function readException(node: TreeValue, path: string): Result<ExceptionRecord | null, EnvelopeRefused> {
  if (node === null) {
    return ok(null);
  }

  if (!isTreeObject(node) || Object.keys(node).length !== EXCEPTION_KEYS.length) {
    return refuse("ErrorFieldInvalid", path, "the exception record has another form");
  }

  const [type, module, message, repr, traceback] = EXCEPTION_KEYS.map((key) => node[key]);

  return isString(type) && isString(module) && isString(message) && isString(repr) && isString(traceback)
    ? ok(Object.freeze({ type, module, message, repr, traceback }))
    : refuse("ErrorFieldInvalid", path, "the exception record has another form");
}

function readMessage(node: TreeValue, path: string): Result<string | null, EnvelopeRefused> {
  return node === null || isString(node) ? ok(node) : refuse("ErrorFieldInvalid", path, "not a string or null");
}

/** The error data: `null`, or a user value of at most `ERROR_DATA_DEPTH_LIMIT` levels with no engine key. */
function readData(node: TreeValue, path: string): Result<JsonValue, DecodeRefused> {
  const data = decodeJsonNode(node, path, 0, ERROR_DATA_DEPTH_LIMIT);

  return data.ok
    ? data
    : refuse("ErrorDataRefused", path, `the data breaks a rule of a user value: ${data.error.kind}`);
}
