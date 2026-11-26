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
    workflow_ctx_from: [],
    allow_failed_deps: false,
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
