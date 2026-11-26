// The conversion of a checked spec to the core's spec data: the generated document type (`workflow_spec.ts`), its
// `toWire`, and the text. The core parses the text (`parse_workflow_spec_data`) and checks every bound once.
//
// The facts that only the core holds come through `ConversionFacts`: the stored options text of each task node (the
// task's text, with the node's deadline), and the retention class key of each queue. On a main thread the addon gives
// them; so the conversion runs on a main thread.

import {
  type SpecJoin,
  type SpecNode,
  type SpecNodeBody,
  type SpecSuccessPolicy,
  SPEC_DATA_VERSION_MAX,
  type WorkflowSpecData,
  workflowSpecDataToWire,
} from "../_catalog/workflow_spec.ts";
import type { Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import type {
  CheckedBody,
  CheckedJoin,
  CheckedNode,
  CheckedPolicy,
  CheckedSpec,
  NodeDeadline,
  NodePriority,
} from "./findings.ts";
import { specDataText, type SpecTextRefused } from "./spec-text.ts";

/** The facts of the app's core that a conversion reads. Each is pure. */
export interface ConversionFacts {
  /** The stored options text of a node of the listed task `taskName`, with the node's deadline when it has one. */
  taskOptionsText(taskName: string, deadline: NodeDeadline): string;
  /** The retention class key of a queue that the app admits. */
  queueClass(queueName: string): string;
  /** The priority of a queue that the app admits, in the core's accepted config. */
  queuePriority(queueName: string): number;
}

/** The spec data document of a checked spec. Pure apart from the facts. */
export function specDataDocument(spec: CheckedSpec, facts: ConversionFacts): WorkflowSpecData {
  return {
    data_version: SPEC_DATA_VERSION_MAX,
    name: spec.name,
    definition_key: { kind: "KEYED", key: spec.definitionKey },
    on_error: spec.onError,
    output: { kind: "NODE", index: spec.output },
    success_policy: successPolicy(spec.successPolicy),
    queue_classes: queueClasses(spec.nodes, facts),
    nodes: spec.nodes.map((node) => specNode(node, facts)),
  };
}

/** The spec data text of a checked spec, or the refusal of a string that has no text. Pure apart from the facts. */
export function specDataTextOf(spec: CheckedSpec, facts: ConversionFacts): Result<string, SpecTextRefused> {
  return specDataText(workflowSpecDataToWire(specDataDocument(spec, facts)));
}

function successPolicy(policy: CheckedPolicy): SpecSuccessPolicy {
  switch (policy.kind) {
    case "noPolicy":
      return { kind: "NO_POLICY" };
    case "cases":
      return {
        kind: "CASES",
        cases: policy.cases.map((required) => ({ required })),
        optional: policy.optional,
      };
    default:
      return unreachable(policy, "successPolicy");
  }
}

/** The class of each task queue, in the order of the nodes that first name it. Pure apart from the facts. */
function queueClasses(nodes: readonly CheckedNode[], facts: ConversionFacts): ReadonlyMap<string, string> {
  const classes = new Map<string, string>();

  for (const node of nodes) {
    if (node.body.kind === "task" && !classes.has(node.body.queue)) {
      classes.set(node.body.queue, facts.queueClass(node.body.queue));
    }
  }

  return classes;
}

function specNode(node: CheckedNode, facts: ConversionFacts): SpecNode {
  return {
    index: node.index,
    node_id: node.nodeId,
    task_name: node.name,
    input_json: node.staticInput,
    dependencies: node.dependencies,
    args_from: node.argsFrom,
    workflow_context_from: node.workflowContextFrom,
    allow_failed_dependencies: node.allowFailedDependencies,
    join: join(node.join),
    body: body(node.name, node.body, facts),
  };
}

function join(value: CheckedJoin): SpecJoin {
  switch (value.kind) {
    case "all":
      return { kind: "ALL" };
    case "any":
      return { kind: "ANY" };
    case "quorum":
      return { kind: "QUORUM", min_success: value.minSuccess };
    default:
      return unreachable(value, "join");
  }
}

/** The priority of a task node: its own, or its queue's. Pure apart from the facts. */
function priorityOf(queue: string, priority: NodePriority, facts: ConversionFacts): number {
  switch (priority.kind) {
    case "given":
      return priority.priority;
    case "queue":
      return facts.queuePriority(queue);
    default:
      return unreachable(priority, "priorityOf");
  }
}

function body(name: string, value: CheckedBody, facts: ConversionFacts): SpecNodeBody {
  switch (value.kind) {
    case "task":
      return {
        kind: "TASK",
        queue: value.queue,
        priority: priorityOf(value.queue, value.priority, facts),
        task_options: { kind: "ENCODED", json: facts.taskOptionsText(name, value.deadline) },
      };
    case "subWorkflow":
      return { kind: "SUB_WORKFLOW", definition_key: value.definitionKey, params: value.params };
    default:
      return unreachable(value, "body");
  }
}
