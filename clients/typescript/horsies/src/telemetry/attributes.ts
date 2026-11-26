// The span attribute keys and values of the package: the same as the Python and Rust clients'. The task and queue keys
// are the label keys of the core's metric catalog.

export { QUEUE_ATTRIBUTE as QUEUE_KEY, TASK_ATTRIBUTE as TASK_NAME_KEY } from "../_catalog/producer_metrics.ts";

/** `messaging.system` of every span. */
export const MESSAGING_SYSTEM = "horsies";
export const MESSAGING_SYSTEM_KEY = "messaging.system";
export const MESSAGING_OPERATION_TYPE_KEY = "messaging.operation.type";
export const MESSAGING_OPERATION_NAME_KEY = "messaging.operation.name";
export const MESSAGING_DESTINATION_NAME_KEY = "messaging.destination.name";
export const MESSAGING_MESSAGE_ID_KEY = "messaging.message.id";
export const ERROR_TYPE_KEY = "error.type";
export const TASK_ID_KEY = "horsies.task.id";

/** The attempt of a run, from 1. */
export const TASK_ATTEMPT_KEY = "horsies.task.attempt";

/** The execution class of the child that ran an attempt. */
export const TASK_EXECUTION_KEY = "horsies.task.execution";
export const WORKER_ID_KEY = "horsies.worker.id";
export const WORKFLOW_ID_KEY = "horsies.workflow.id";
export const WORKFLOW_NAME_KEY = "horsies.workflow.name";

/** The index of the workflow node that links a task. */
export const WORKFLOW_NODE_KEY = "horsies.workflow.node";

/** The top-level workflow of the tree of the node that links a task. */
export const WORKFLOW_ROOT_ID_KEY = "horsies.workflow.root_id";

/** Who made an attempt span: `worker` for the span of an attempt that the worker ended. */
export const SPAN_SOURCE_KEY = "horsies.span.source";
export const WORKER_SPAN_SOURCE = "worker";

/** `messaging.operation.type` of a send and a start, and `messaging.operation.name` of a task send. */
export const SEND_OPERATION = "send";

/** `messaging.operation.name` of a workflow start. */
export const START_OPERATION = "start";

/** `messaging.operation.type` of an attempt. */
export const PROCESS_OPERATION = "process";

/** `messaging.operation.name` of an attempt. */
export const RUN_OPERATION = "run";
