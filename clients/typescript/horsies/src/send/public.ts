// The public names of sends. `src/index.ts` exports this module.

export type { CoreCause, NoCause } from "../_catalog/error_detail.ts";

export type {
  TaskSendEnqueueFailed,
  TaskSendIdempotencyKeyConflict,
  TaskSendPayload,
  TaskSendPayloadMismatch,
  TaskSendPayloadTooLarge,
  TaskSendSuppressed,
  TaskSendValidationFailed,
} from "../_catalog/task_send_errors.ts";

export type { RetryableSendError, TaskSendError } from "./errors.ts";

export type { Retention, SendOptions } from "./options.ts";
