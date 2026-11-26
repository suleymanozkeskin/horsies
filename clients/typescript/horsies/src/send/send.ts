// The send path: `app.send` and `app.retrySend`.
//
// A send: the contract is listed; sends are not suppressed; the options and the input pass their checks (the input
// with a read-back); the core prepares the facts (task id, options text, retention, payload bound, the priority of the
// queue in the accepted config); one enqueue with
// `FIRST`, wrapped by the send observer; with `resendOnTransientErr`, a retryable `ENQUEUE_FAILED` is sent again with
// `REPEAT`, at most `RESEND_COUNT` times. Every refusal before the enqueue writes nothing.

import type { CoreAccess } from "../app/core-access.ts";
import type { SendObserver } from "../app/observers.ts";
import type { SuppressionState } from "../app/suppression.ts";
import type { TaskSendPayload } from "../_catalog/task_send_errors.ts";
import { encodeParametersWithReadBack } from "../codec/read-back.ts";
import { refusalText } from "../codec/refusal.ts";
import type { CompiledContract } from "../contract/compile.ts";
import { contractRecord } from "../contract/definition.ts";
import { type TaskId, taskIdFromCore } from "../contract/ids.ts";
import type { EnqueueRequest, OptionalText, RetentionRecord, SendKind, SendPriority } from "../native/sends/addon.ts";
import type { TraceArgument } from "../native/values/records.ts";
import { readFormOfCompiled, type TaskHandle, taskHandleOf } from "../read/handle.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { DEFAULT_QUEUE_PRIORITY, RESEND_COUNT, RESEND_FIRST_WAIT_MS, RESEND_LONGEST_WAIT_MS } from "./bounds.ts";
import { enqueue, prepareSend } from "./core-calls.ts";
import {
  preconditionFailed,
  type RetryableSendError,
  ruleRefused,
  type SendFailure,
  sendErrorOfRaw,
  suppressedError,
  type TaskSendError,
  validationFailed,
} from "./errors.ts";
import { checkSendOptions, epochMicrosTowardPast, type SendOptions } from "./options.ts";

/** The queue that each send of a task carries. The priority of the queue comes from the core's accepted config. */
export type QueueRoute = { readonly queueName: string };

/** A contract of the app's list: its compiled form and its queue route. */
export type ListedTask = { readonly compiled: CompiledContract; readonly route: QueueRoute };

/** Whether the app lists a contract. */
export type ListedLookup = { readonly kind: "listed"; readonly task: ListedTask } | { readonly kind: "notListed" };

/** The app's contract list, as the send path reads it. */
export interface ContractLookup {
  /** The listed task of this contract object. */
  byContract(contract: object): ListedLookup;
  /** The listed task of this task name (a retry of an error of another app instance). */
  byName(taskName: string): ListedLookup;
}

/** What a send reads from its app. */
export interface SendContext {
  readonly contracts: ContractLookup;
  readonly resendOnTransientErr: boolean;
  readonly core: CoreAccess;
  readonly observer: SendObserver;
  readonly suppression: SuppressionState;
  /** Wait `milliseconds` before a resend. */
  readonly pause: (milliseconds: number) => Promise<void>;
  /** The sent instant of a new send. */
  readonly now: () => Temporal.Instant;
}

type SendOutcome<O, E extends string> = Result<TaskHandle<O, E>, TaskSendError<O, E>>;

/** Send `input` to the task of `contract`. Effects: one or more enqueues through the core. */
export async function sendTask<O, E extends string>(
  context: SendContext,
  contract: object,
  input: object,
  options: SendOptions,
): Promise<SendOutcome<O, E>> {
  const taskName = contractRecord(contract).name;
  const listed = context.contracts.byContract(contract);

  if (listed.kind === "notListed") {
    return err(notListed(taskName));
  }

  const suppressed = suppression(context);

  if (suppressed.kind === "suppressed") {
    return err(suppressed.error);
  }

  const request = await preparedRequest(context, listed.task, input, options);

  return request.ok ? sentHandle<O, E>(context, listed.task, request.value, "FIRST") : request;
}

/**
 * Send again the payload of a retryable error (`ENQUEUE_FAILED`, `IDEMPOTENCY_KEY_CONFLICT`), under the same task id,
 * with `REPEAT`. A key conflict before the reservation ends gives the same conflict again.
 */
export async function retrySend<O, E extends string>(
  context: SendContext,
  error: RetryableSendError<O, E>,
): Promise<SendOutcome<O, E>> {
  // A JavaScript caller can pass another error than the type allows.
  const code: string = error.code;

  if (!isRetrySendCode(code)) {
    const message = `retrySend takes ENQUEUE_FAILED and IDEMPOTENCY_KEY_CONFLICT only; ${code} is not retryable`;

    return err(validationFailed(message, ruleRefused(message)));
  }

  const listed = context.contracts.byName(error.payload.taskName);

  if (listed.kind === "notListed") {
    return err(notListed(error.payload.taskName));
  }

  const suppressed = suppression(context);

  if (suppressed.kind === "suppressed") {
    return err(suppressed.error);
  }

  return sentHandle<O, E>(context, listed.task, requestOfPayload(error.taskId, error.payload), "REPEAT");
}

/** Whether `retrySend` takes an error of the code `code`, a text that a JavaScript caller can give. Pure. */
function isRetrySendCode(code: string): boolean {
  switch (code) {
    case "ENQUEUE_FAILED":
    case "IDEMPOTENCY_KEY_CONFLICT":
      return true;
    default:
      return false;
  }
}

function notListed(taskName: string): SendFailure {
  const message = `the task ${taskName} is not in the tasks list of this app; add its contract to Horsies.create({ tasks })`;

  return validationFailed(message, preconditionFailed(message));
}

function suppression(
  context: SendContext,
): { readonly kind: "open" } | { readonly kind: "suppressed"; readonly error: SendFailure } {
  const current = context.suppression.current();

  switch (current.kind) {
    case "open":
      return current;
    case "suppressed":
      return { kind: "suppressed", error: suppressedError(`sends are suppressed in this process (${current.reason})`) };
    default: {
      const unexpected: never = current;

      throw new TypeError(`send: a suppression outside its type: ${String(unexpected)}`);
    }
  }
}

/** The request of a new send, or the error of a refused option, input or fact. Effects: the core's prepare step. */
async function preparedRequest(
  context: SendContext,
  task: ListedTask,
  input: object,
  options: SendOptions,
): Promise<Result<EnqueueRequest, SendFailure>> {
  const { compiled, route } = task;
  const taskName = compiled.record.name;
  const checked = checkSendOptions(options);

  if (!checked.ok) {
    const message = `send of ${taskName}: ${checked.error.option}: ${checked.error.detail}`;

    return err(validationFailed(message, ruleRefused(message)));
  }

  if (compiled.send.kind === "workflowNodeOnly") {
    const message = `send of ${taskName}: the task takes an upstream result (${compiled.send.upstream.join(", ")}); only a workflow node can run it`;

    return err(validationFailed(message, ruleRefused(message)));
  }

  const text = encodeParametersWithReadBack(compiled.send.parameters, input);

  if (!text.ok) {
    const message = `send of ${taskName}: the input does not fit the contract: ${refusalText(text.error)}`;

    return err(validationFailed(message, ruleRefused(message)));
  }

  const prepared = await prepareSend(context.core, taskName, {
    retention: checked.value.retention,
    deadline: checked.value.deadline,
    inputBytes: Buffer.byteLength(text.value, "utf8"),
  });

  switch (prepared.kind) {
    case "refused":
      return err(sendErrorOfRaw(prepared.error));
    case "seatRelayFull":
      return err(prepared.error);
    case "prepared":
      break;
    default: {
      const unexpected: never = prepared;

      throw new TypeError(`send: a prepare outside its type: ${String(unexpected)}`);
    }
  }

  const request: EnqueueRequest = {
    taskId: prepared.taskId,
    taskName,
    queueName: route.queueName,
    priority: sendPriority(prepared.priority),
    inputJson: text.value,
    sentAtMicros: epochMicrosTowardPast(context.now()),
    deadline: checked.value.deadline,
    delay: checked.value.delay,
    taskOptions: { kind: "text", text: prepared.taskOptions },
    idempotencyKey: checked.value.idempotencyKey,
    retention: prepared.retention,
  };

  return ok(Object.freeze(request));
}

/** The priority of a prepared send: the client's priority of the default queue, or the configured one. Pure. */
export function sendPriority(priority: SendPriority): number {
  switch (priority.kind) {
    case "defaultQueue":
      return DEFAULT_QUEUE_PRIORITY;
    case "configured":
      return priority.priority;
    default: {
      const unexpected: never = priority;

      throw new TypeError(`send: a priority outside its type: ${String(unexpected)}`);
    }
  }
}

/** The request of a resend of a payload. Pure. */
function requestOfPayload(taskId: string, payload: TaskSendPayload): EnqueueRequest {
  const request: EnqueueRequest = {
    taskId,
    taskName: payload.taskName,
    queueName: payload.queueName,
    priority: payload.priority,
    inputJson: payload.inputJson ?? EMPTY_INPUT,
    sentAtMicros: epochMicrosTowardPast(payload.sentAt),
    deadline:
      payload.goodUntil === null ? { kind: "noDeadline" } : { kind: "until", epochMicros: epochMicrosTowardPast(payload.goodUntil) },
    delay: payload.enqueueDelaySeconds === null ? { kind: "noDelay" } : { kind: "seconds", seconds: payload.enqueueDelaySeconds },
    taskOptions: optionalText(payload.taskOptions),
    idempotencyKey: optionalText(payload.idempotencyKey),
    retention: retentionOf(payload.retentionClassKey),
  };

  return Object.freeze(request);
}

/** The input text that the core stores for a payload with no input (Python's `None`). */
const EMPTY_INPUT = "{}";

function optionalText(text: string | null): OptionalText {
  return text === null ? { kind: "none" } : { kind: "text", text };
}

/** The retention of a payload: `null` is the core's "retain forever". */
function retentionOf(key: string | null): RetentionRecord {
  return key === null ? { kind: "forever" } : { kind: "classKey", key };
}

/** The handle of a sent request, or its send error. */
async function sentHandle<O, E extends string>(
  context: SendContext,
  task: ListedTask,
  request: EnqueueRequest,
  firstKind: SendKind,
): Promise<SendOutcome<O, E>> {
  const facts = { taskName: request.taskName, queueName: request.queueName, taskId: request.taskId };
  const sent = await context.observer.observeSend(facts, (trace) => enqueueWithResends(context, request, firstKind, trace));

  if (!sent.ok) {
    return sent;
  }

  return ok(taskHandleOf<O, E>(context.core, readFormOfCompiled(task.compiled), sent.value));
}

/**
 * One enqueue; with `resendOnTransientErr`, a retryable `ENQUEUE_FAILED` is sent again with `REPEAT` after
 * `RESEND_FIRST_WAIT_MS`, doubled each time, at most `RESEND_LONGEST_WAIT_MS`, at most `RESEND_COUNT` times.
 */
export async function enqueueWithResends(
  context: SendContext,
  request: EnqueueRequest,
  firstKind: SendKind,
  trace: TraceArgument,
): Promise<Result<TaskId, SendFailure>> {
  let sent = await enqueueOnce(context, request, firstKind, trace);

  for (let resend = 0; resend < RESEND_COUNT && context.resendOnTransientErr && isTransient(sent); resend += 1) {
    await context.pause(Math.min(RESEND_FIRST_WAIT_MS * 2 ** resend, RESEND_LONGEST_WAIT_MS));
    sent = await enqueueOnce(context, request, "REPEAT", trace);
  }

  return sent;
}

async function enqueueOnce(
  context: SendContext,
  request: EnqueueRequest,
  sendKind: SendKind,
  trace: TraceArgument,
): Promise<Result<TaskId, SendFailure>> {
  const outcome = await enqueue(context.core, request, sendKind, trace);

  switch (outcome.kind) {
    case "enqueued":
      return ok(taskIdFromCore(outcome.taskId, "enqueue"));
    case "failed":
      return err(sendErrorOfRaw(outcome.error));
    case "seatRelayFull":
      return err(outcome.error);
    default: {
      const unexpected: never = outcome;

      throw new TypeError(`enqueue: an outcome outside its type: ${String(unexpected)}`);
    }
  }
}

function isTransient(sent: Result<TaskId, SendFailure>): boolean {
  return !sent.ok && sent.error.code === "ENQUEUE_FAILED" && sent.error.retryable;
}
