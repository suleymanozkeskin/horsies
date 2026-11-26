// The public names of sends. `src/index.ts` exports this module.

export type {
  CoreCause,
  CoreConnectionError,
  CoreContractViolation,
  CoreFailure,
  CorePreconditionFailed,
  CoreServerError,
  HistoryContractError,
  JsonUnreadable,
  NoCause,
  OutcomeDecodeError,
  RuleRefused,
} from "../_catalog/error_detail.ts";

export { DEFAULT_RETENTION_CLASS_KEY, type RetentionClassKey } from "../_catalog/retention_class.ts";

export type {
  TaskSendEnqueueFailed,
  TaskSendIdempotencyKeyConflict,
  TaskSendPayload,
  TaskSendPayloadMismatch,
  TaskSendPayloadTooLarge,
  TaskSendSuppressed,
  TaskSendValidationFailed,
} from "../_catalog/task_send_errors.ts";

export type {
  RetryableSendError,
  SendFailure,
  SentTypes,
  TaskSendError,
  TaskSendKeyConflict,
  WithSentTypes,
} from "./errors.ts";

export type { SeatRelayFull } from "../seats/relay-full.ts";

export { SeatRelayCode } from "../seats/relay-full.ts";

export type { Retention, SendOptions } from "./options.ts";
