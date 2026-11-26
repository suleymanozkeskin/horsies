// Bounds, spellings and code lists of the core's stored forms. Each constant names its source in the core. The
// codegen output takes them over (#2372).

/**
 * The deepest nesting of arrays and objects in a stored text (core `MAX_JSON_DEPTH`,
 * `core/src/core/codec/strict_read.rs`). The outermost array or object is level 1.
 */
export const MAX_JSON_DEPTH = 300;

/** The most digits of a stored integer (core `MAX_INT_DIGITS`, `core/src/core/codec/strict_read.rs`). */
export const MAX_INT_DIGITS = 4300;

/**
 * The deepest nesting of a value that user code builds (core `MAX_VALUE_JSON_DEPTH`,
 * `core/src/core/codec/user_value.rs`). The outermost array or object is level 1.
 */
export const MAX_VALUE_JSON_DEPTH = 200;

/** Key prefixes that only the engine writes (core `ENGINE_KEY_PREFIXES`, `core/src/core/codec/user_value.rs`). */
export const ENGINE_KEY_PREFIXES: readonly string[] = Object.freeze(["__h_", "__horsies_"]);

/** The tag key of a built-in error code (core `BUILTIN_CODE_KEY`, `core/src/core/codec/user_value.rs`). */
export const BUILTIN_CODE_KEY = "__builtin_task_code__";

/** The marker key of a stored task result (core `TASK_RESULT_ENVELOPE_KEY`, `core/src/core/task/result.rs`). */
export const TASK_RESULT_ENVELOPE_KEY = "__h_task_result__";

/** The deepest `data` of a stored task error (core `ERROR_DATA_DEPTH_LIMIT`, `core/src/core/task/error.rs`). */
export const ERROR_DATA_DEPTH_LIMIT: number = MAX_VALUE_JSON_DEPTH - 1;

/**
 * The deepest `output` of the summary of a sub-workflow error (core `SUMMARY_OUTPUT_DEPTH_LIMIT`,
 * `core/src/core/task/error.rs`).
 */
export const SUMMARY_OUTPUT_DEPTH_LIMIT: number = MAX_VALUE_JSON_DEPTH - 2;

/** The largest count of a sub-workflow summary: the core stores each count as an `i32`. */
export const SUMMARY_COUNT_MAX = 2_147_483_647;

/** The smallest count of a sub-workflow summary (the `i32` range). */
export const SUMMARY_COUNT_MIN = -2_147_483_648;

/**
 * The library's task error codes: the four built-in code families of the core (`BuiltInTaskCode`,
 * `core/src/core/task/error.rs`: operational, contract, retrieval, outcome).
 */
export const LIBRARY_TASK_CODES = [
  "UNHANDLED_EXCEPTION",
  "TASK_EXCEPTION",
  "WORKER_CRASHED",
  "BROKER_ERROR",
  "WORKER_RESOLUTION_ERROR",
  "WORKER_SERIALIZATION_ERROR",
  "RESULT_DESERIALIZATION_ERROR",
  "WORKFLOW_ENQUEUE_FAILED",
  "SUBWORKFLOW_LOAD_FAILED",
  "WORKFLOW_RECOVERY_LIMIT_REACHED",
  "SUBWORKFLOW_DEPTH_LIMIT_REACHED",
  "TASK_BLOCKED_LOOP",
  "NEIGHBOUR_BLOCKED_LOOP",
  "WORKFLOW_START_INCOMPLETE",
  "RETURN_TYPE_MISMATCH",
  "WORKFLOW_CTX_MISSING_ID",
  "NO_TYPE_AVAILABLE",
  "WAIT_TIMEOUT",
  "TASK_NOT_FOUND",
  "WORKFLOW_NOT_FOUND",
  "RESULT_NOT_AVAILABLE",
  "RESULT_NOT_READY",
  "ASYNC_CONTEXT",
  "INVALID_INPUT",
  "WAIT_INSIDE_TASK",
  "TASK_CANCELLED",
  "TASK_EXPIRED",
  "TASK_TIMEOUT",
  "WORKFLOW_PAUSED",
  "WORKFLOW_EXPIRED",
  "WORKFLOW_FAILED",
  "WORKFLOW_CANCELLED",
  "UPSTREAM_SKIPPED",
  "SUBWORKFLOW_FAILED",
  "WORKFLOW_SUCCESS_CASE_NOT_MET",
  "WORKFLOW_OUTPUT_SKIPPED",
] as const;

/** Code texts that are reserved and are not task codes (core `RESERVED_NON_TASK_CODES`). */
export const RESERVED_NON_TASK_CODES = ["WORKFLOW_STOPPED"] as const;

/** The statuses of a workflow (core `WorkflowStatus`, `core/src/core/workflow/status.rs`). */
export const WORKFLOW_STATUSES = [
  "PENDING",
  "RUNNING",
  "COMPLETED",
  "FAILED",
  "PAUSED",
  "CANCELLED",
  "EXPIRED",
] as const;

// `horsies` exports LIBRARY_TASK_CODES and WORKFLOW_STATUSES (the public types name them), so both lists are frozen.
void Object.freeze(LIBRARY_TASK_CODES);

void Object.freeze(WORKFLOW_STATUSES);

/** The key of the replacement `data` of a task error (core `DATA_REFUSED_KEY`, `core/src/core/task/error.rs`). */
export const DATA_REFUSED_KEY = "data_refused";
