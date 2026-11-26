// Gate 2 of the spec checks for each node: the app lists its task or child workflow, the node options (queue,
// priority, deadline, join, context sources) and the node's input. Every finding of every node is reported.

import { ErrorCode } from "../_catalog/error_codes.ts";
import { MIN_SUCCESS_MIN, QUEUE_PRIORITY_MAX, QUEUE_PRIORITY_MIN } from "../_catalog/workflow_spec.ts";
import { DEFAULT_QUEUE_ROUTE } from "../app/contract-list.ts";
import { isObjectLike } from "../codec/values.ts";
import type { ContextUse } from "../contract/define-task.ts";
import { contractRecord } from "../contract/definition.ts";
import type { QueueRoute } from "../send/send.ts";
import { epochMicrosTowardPast } from "../send/options.ts";
import type { NonEmpty } from "../wire/builders.ts";
import type { ObjectKind } from "../wire/kinds.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import { type BuildSession, nodeSlot, type RecordedNode, type RecordedOptions } from "./builder.ts";
import type { SpecAppView } from "./checks.ts";
import {
  type CheckedBody,
  type CheckedJoin,
  type CheckedNode,
  finding,
  type NodeDeadline,
  type SpecFinding,
} from "./findings.ts";
import { type CheckedInput, checkNodeInput, type SourceOutput } from "./node-input.ts";

/** What a node runs, for the reads of a handle: a task contract, or a child workflow contract. */
export type NodeTarget =
  | { readonly kind: "task"; readonly contract: object }
  | { readonly kind: "subWorkflow"; readonly workflow: object };

/** The checked nodes of a build and the target of each node id. */
export type CheckedNodes = {
  readonly nodes: readonly CheckedNode[];
  readonly targets: ReadonlyMap<string, NodeTarget>;
};

/** The first instant of Python's `datetime` range, 0001-01-01T00:00:00Z, in epoch microseconds. */
export const DEADLINE_MIN_EPOCH_MICROS = -62_135_596_800_000_000n;

/** The last instant of Python's `datetime` range, 9999-12-31T23:59:59.999999Z, in epoch microseconds. */
export const DEADLINE_MAX_EPOCH_MICROS = 253_402_300_799_999_999n;

/** The facts of one node that its checks share. */
type NodeContext = {
  readonly index: number;
  readonly place: string;
  readonly session: BuildSession;
  readonly view: SpecAppView;
  readonly outputs: readonly SourceOutput[];
};

type NodeChecked = Result<CheckedNode, NonEmpty<SpecFinding>>;

/** Gate 2 of every node, in index order: the checked nodes, or every finding. Pure. */
export function checkNodes(
  nodes: readonly RecordedNode[],
  session: BuildSession,
  view: SpecAppView,
): Result<CheckedNodes, NonEmpty<SpecFinding>> {
  const outputs = nodes.map((node) => sourceOutputOf(node, view));
  const checked: CheckedNode[] = [];
  const findings: SpecFinding[] = [];
  const targets = new Map<string, NodeTarget>();

  nodes.forEach((node, index) => {
    const context: NodeContext = { index, place: `nodes[${index}] ${node.options.id}`, session, view, outputs };
    const result = node.kind === "task" ? checkTaskNode(context, node) : checkSubWorkflowNode(context, node);

    if (result.ok) {
      checked.push(result.value);
    } else {
      findings.push(...result.error);
    }

    targets.set(
      node.options.id,
      node.kind === "task"
        ? { kind: "task", contract: node.contract }
        : { kind: "subWorkflow", workflow: node.workflow },
    );
  });

  const [first, ...rest] = findings;

  return first === undefined ? ok({ nodes: checked, targets }) : err([first, ...rest]);
}

/** What a node gives to an `h.resultOf` parameter: the output kind of a listed task, else not known here. */
function sourceOutputOf(node: RecordedNode, view: SpecAppView): SourceOutput {
  if (node.kind === "subWorkflow") {
    return { kind: "notKnown" };
  }

  const listed = view.task(node.contract);

  return listed.kind === "listed" ? { kind: "known", output: listed.task.compiled.output } : { kind: "notKnown" };
}

type TaskRecordedNode = Extract<RecordedNode, { readonly kind: "task" }>;

type SubWorkflowRecordedNode = Extract<RecordedNode, { readonly kind: "subWorkflow" }>;

function checkTaskNode(context: NodeContext, node: TaskRecordedNode): NodeChecked {
  const listed = context.view.task(node.contract);

  if (listed.kind === "notListed") {
    return err([
      finding(
        ErrorCode.WORKFLOW_UNREGISTERED_TASK,
        context.place,
        `the task ${contractRecord(node.contract).name} is not in the tasks list of the app`,
        ["add its contract to Horsies.create({ tasks })"],
      ),
    ]);
  }

  const compiled = listed.task.compiled;
  const body = taskBody(context, node.options, listed.task.route);
  const input = checkNodeInput(inputFacts(context, compiled.parameters, node.input, node.options));
  const links = linkFindings(context, node, compiled.record.context, input);
  const findings = [...(body.ok ? [] : body.error), ...(input.ok ? [] : input.error), ...links];
  const [first, ...rest] = findings;

  if (first !== undefined || !body.ok || !input.ok) {
    return err(nonEmpty(first, rest));
  }

  return ok(checkedNode(context, node, compiled.record.name, input.value, body.value));
}

function checkSubWorkflowNode(context: NodeContext, node: SubWorkflowRecordedNode): NodeChecked {
  const listed = context.view.workflow(node.workflow);

  if (listed.kind === "notListed") {
    return err([
      finding(
        ErrorCode.WORKFLOW_MISSING_REQUIRED_PARAMS,
        context.place,
        "the child workflow is not in the workflows list of the app",
        ["add its contract to Horsies.create({ workflows })"],
      ),
    ]);
  }

  const takesParams = listed.record.paramNames.length > 0;

  const input = takesParams
    ? checkNodeInput(inputFacts(context, listed.params, node.input, node.options))
    : noParams(context, node.input);

  const links = linkFindings(context, node, "noContext", input);
  const findings = [...(input.ok ? [] : input.error), ...links];
  const [first, ...rest] = findings;

  if (first !== undefined || !input.ok) {
    return err(nonEmpty(first, rest));
  }

  const body: CheckedBody = {
    kind: "subWorkflow",
    definitionKey: listed.record.definitionKey,
    params: takesParams ? "GIVEN" : "NOT_GIVEN",
  };

  return ok(checkedNode(context, node, listed.record.name, input.value, body));
}

/** The input of a child that takes no params: `{}` only (`HRS-022`). */
function noParams(context: NodeContext, input: object): Result<CheckedInput, NonEmpty<SpecFinding>> {
  return Object.keys(input).length === 0
    ? ok({ staticInput: "{}", argsFrom: new Map() })
    : err([
        finding(
          ErrorCode.WORKFLOW_SUBWORKFLOW_PARAMS_REQUIRE_BUILD_WITH,
          `${context.place} input`,
          "the child workflow takes no params",
          ["give {} to a child with params: {}"],
        ),
      ]);
}

function inputFacts(context: NodeContext, parameters: ObjectKind, input: object, options: RecordedOptions) {
  return {
    place: context.place,
    parameters,
    input,
    join: joinOf(options),
    session: context.session,
    sourceOutput: (index: number): SourceOutput => context.outputs[index] ?? { kind: "notKnown" },
  };
}

function nonEmpty(first: SpecFinding | undefined, rest: readonly SpecFinding[]): NonEmpty<SpecFinding> {
  if (first === undefined) {
    throw new TypeError("checkNodes: a refused node with no finding");
  }

  return [first, ...rest];
}

function checkedNode(
  context: NodeContext,
  node: RecordedNode,
  name: string,
  input: CheckedInput,
  body: CheckedBody,
): CheckedNode {
  return {
    index: context.index,
    nodeId: node.options.id,
    name,
    staticInput: input.staticInput,
    dependencies: dependenciesOf(context.session, node, input.argsFrom),
    argsFrom: input.argsFrom,
    ctxFrom: (node.options.ctxFrom ?? []).map((source) => source.id),
    allowFailedDeps: node.options.allowFailedDeps === true,
    join: joinOf(node.options),
    body,
  };
}

/** The join of the options; left out, `all`. A join of another kind is refused by `linkFindings`. Pure. */
function joinOf(options: RecordedOptions): CheckedJoin {
  return options.join ?? { kind: "all" };
}

/** The node indexes that a node waits for: its parameter sources, then `waitsFor`, each node once. Pure. */
function dependenciesOf(
  session: BuildSession,
  node: RecordedNode,
  argsFrom: ReadonlyMap<string, number>,
): readonly number[] {
  const indexes = [...argsFrom.values(), ...(node.options.waitsFor ?? []).map((source) => indexOf(session, source))];

  return [...new Set(indexes)];
}

/** The index of a node of `session`; gate 1 checked that each named node is one. */
function indexOf(session: BuildSession, value: object): number {
  const slot = nodeSlot(value);

  if (slot.kind !== "node" || slot.session !== session) {
    throw new TypeError("checkNodes: a node of another build passed gate 1");
  }

  return slot.index;
}

/** The parameter sources of an input, whether or not the input check passed: the nodes among its members. */
function sourcesOf(session: BuildSession, input: object): readonly number[] {
  return Object.values(input).flatMap((value) =>
    isObjectLike(value) && nodeSlot(value).kind === "node" ? [indexOf(session, value)] : [],
  );
}

/** The join (`HRS-013`) and context (`HRS-009`, `HRS-010`) findings of a node. */
function linkFindings(
  context: NodeContext,
  node: RecordedNode,
  use: ContextUse,
  input: Result<CheckedInput, NonEmpty<SpecFinding>>,
): readonly SpecFinding[] {
  const sources = input.ok ? [...input.value.argsFrom.values()] : sourcesOf(context.session, node.input);
  const waits = (node.options.waitsFor ?? []).map((source) => indexOf(context.session, source));
  const dependencies = new Set([...sources, ...waits]);

  return [
    ...joinFindings(context.place, joinOf(node.options), dependencies.size),
    ...contextFindings(context, node, use, dependencies),
  ];
}

function joinFindings(place: string, join: CheckedJoin, dependencies: number): readonly SpecFinding[] {
  switch (join.kind) {
    case "all":
    case "any":
      return [];
    case "quorum":
      return Number.isSafeInteger(join.minSuccess) &&
        join.minSuccess >= MIN_SUCCESS_MIN &&
        join.minSuccess <= dependencies
        ? []
        : [
            finding(
              ErrorCode.WORKFLOW_INVALID_JOIN,
              `${place} join`,
              `minSuccess ${String(join.minSuccess)} with ${dependencies} dependencies`,
              [`set minSuccess to an integer from ${MIN_SUCCESS_MIN} to the number of dependencies`],
            ),
          ];
    default:
      return [finding(ErrorCode.WORKFLOW_INVALID_JOIN, `${place} join`, "a join is all, any or quorum", [])];
  }
}

function contextFindings(
  context: NodeContext,
  node: RecordedNode,
  use: ContextUse,
  dependencies: ReadonlySet<number>,
): readonly SpecFinding[] {
  const sources = node.options.ctxFrom ?? [];

  if (sources.length === 0) {
    return [];
  }

  if (use === "noContext") {
    return [
      finding(
        ErrorCode.WORKFLOW_CTX_PARAM_MISSING,
        `${context.place} ctxFrom`,
        "ctxFrom is given, but the task takes no workflow context",
        ["define the task with workflowContext: true, or remove ctxFrom"],
      ),
    ];
  }

  return sources
    .filter((source) => !dependencies.has(indexOf(context.session, source)))
    .map((source) =>
      finding(
        ErrorCode.WORKFLOW_INVALID_CTX_FROM,
        `${context.place} ctxFrom`,
        `the context source "${source.id}" is not a dependency`,
        ["add the node to waitsFor"],
      ),
    );
}

/** The task body of a node: the queue, the priority and the deadline (`HRS-014`, `HRS-034`). */
function taskBody(
  context: NodeContext,
  options: RecordedOptions,
  taskRoute: QueueRoute,
): Result<CheckedBody, NonEmpty<SpecFinding>> {
  const route = options.queue === undefined ? ok(taskRoute) : admittedRoute(context, options.queue);
  const priority = options.priority ?? (route.ok ? route.value.priority : DEFAULT_QUEUE_ROUTE.priority);
  const deadline = deadlineOf(context, options.goodUntil);

  const findings = [
    ...(route.ok ? [] : [route.error]),
    ...priorityFindings(context.place, priority),
    ...(deadline.ok ? [] : [deadline.error]),
  ];

  const [first, ...rest] = findings;

  if (first !== undefined || !route.ok || !deadline.ok) {
    return err(nonEmpty(first, rest));
  }

  return ok({ kind: "task", queue: route.value.queueName, priority, deadline: deadline.value });
}

/** `HRS-034` for a priority that is not an integer from 1 to 100. Pure. */
function priorityFindings(place: string, priority: number): readonly SpecFinding[] {
  if (Number.isSafeInteger(priority) && priority >= QUEUE_PRIORITY_MIN && priority <= QUEUE_PRIORITY_MAX) {
    return [];
  }

  const detail = `the priority ${String(priority)} is not an integer from ${QUEUE_PRIORITY_MIN} to ${QUEUE_PRIORITY_MAX}`;

  return [finding(ErrorCode.WORKFLOW_INVALID_PRIORITY, `${place} priority`, detail, [])];
}

/** The route of a queue that the node names: one that the app admits, else `HRS-014`. */
function admittedRoute(context: NodeContext, queue: string): Result<QueueRoute, SpecFinding> {
  const queues = context.view.queues;
  const detail = `the queue "${queue}" is not a queue of the app`;

  const refused = finding(ErrorCode.WORKFLOW_UNRESOLVED_QUEUE, `${context.place} queue`, detail, [
    "name one of the customQueues, or leave queue out",
  ]);

  switch (queues.kind) {
    case "default":
      return queue === DEFAULT_QUEUE_ROUTE.queueName ? ok(DEFAULT_QUEUE_ROUTE) : err(refused);
    case "custom": {
      const found = queues.queues.get(queue);

      return found === undefined ? err(refused) : ok({ queueName: found.name, priority: found.priority });
    }

    default:
      return unreachable(queues, "admittedRoute");
  }
}

/** The deadline of a node, cut toward the past to the microsecond; an instant outside Python's range is refused. */
function deadlineOf(context: NodeContext, goodUntil: Temporal.Instant | undefined): Result<NodeDeadline, SpecFinding> {
  if (goodUntil === undefined) {
    return ok({ kind: "noDeadline" });
  }

  const epochMicros = epochMicrosTowardPast(goodUntil);

  return epochMicros >= DEADLINE_MIN_EPOCH_MICROS && epochMicros <= DEADLINE_MAX_EPOCH_MICROS
    ? ok({ kind: "until", epochMicros })
    : err(finding("FIELD_BOUND", `${context.place} goodUntil`, "the deadline is outside the years 1 to 9999", []));
}
