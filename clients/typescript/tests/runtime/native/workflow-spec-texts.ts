// Spec data texts of the native workflow tests, in the form that the package writes and the core parses.

/** The spec data text of one sub-workflow root of the child `tests.strict_child.v1` with the stored input `input`. */
export function subWorkflowRootText(input: string): string {
  const node = {
    index: 0,
    node_id: "child",
    task_name: "strict_child",
    input_json: input,
    dependencies: [],
    args_from: {},
    workflow_context_from: [],
    allow_failed_dependencies: false,
    join: { kind: "ALL" },
    body: { kind: "SUB_WORKFLOW", definition_key: "tests.strict_child.v1", params: "GIVEN" },
  };

  return JSON.stringify({
    data_version: 1,
    name: "strict_parent",
    definition_key: { kind: "KEYED", key: "tests.strict_parent.v1" },
    on_error: "FAIL",
    output: { kind: "NODE", index: 0 },
    success_policy: { kind: "NO_POLICY" },
    queue_classes: {},
    nodes: [node],
  });
}

/** The execution class and the stored `timeout_ms` of the one task node of `taskNodeTimeText`. */
export interface NodeTimeFacts {
  readonly execution: "ISOLATED" | "SHARED_ASYNC" | "SHARED_THREAD";
  readonly timeoutMs: number | null | "absent";
}

/** The `cancel_grace_ms` of a `SHARED_ASYNC` node: the class requires it, and the checks of these tests are about the timeout. */
const SHARED_ASYNC_GRACE_MS = 1000;

/** The spec data text of one task node whose stored options hold the class and the timeout of `facts`. */
export function taskNodeTimeText(facts: NodeTimeFacts): string {
  const options = {
    retry_policy: null,
    good_until: null,
    ...(facts.timeoutMs === "absent" ? {} : { timeout_ms: facts.timeoutMs }),
    execution: facts.execution,
    cancel_grace_ms: facts.execution === "SHARED_ASYNC" ? SHARED_ASYNC_GRACE_MS : null,
  };

  const node = {
    index: 0,
    node_id: "work",
    task_name: "node_time_task",
    input_json: "{}",
    dependencies: [],
    args_from: {},
    workflow_context_from: [],
    allow_failed_dependencies: false,
    join: { kind: "ALL" },
    body: { kind: "TASK", queue: "default", priority: 100, task_options: { kind: "ENCODED", json: JSON.stringify(options) } },
  };

  return JSON.stringify({
    data_version: 1,
    name: "node_time",
    definition_key: { kind: "KEYED", key: "tests.node_time.v1" },
    on_error: "FAIL",
    output: { kind: "NODE", index: 0 },
    success_policy: { kind: "NO_POLICY" },
    queue_classes: { default: "standard_30d" },
    nodes: [node],
  });
}
