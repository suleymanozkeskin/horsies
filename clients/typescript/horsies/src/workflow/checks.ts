// The spec checks of a workflow build, in two gates, with the codes of the Rust client (`spec.rs`):
//
// 1. The name, the node count, the node ids, and the references: every node that a node, the output or the success
//    policy names is a node of this build. A finding here stops the checks.
// 2. Every other check, with all findings together: the node options (queue, priority, deadline, join, context
//    sources), the contracts and child workflows that the app lists, the success policy, and each node's input.
//
// No cycle check is needed: a node can name only nodes that the build made before it.

import { ErrorCode } from "../_catalog/error_codes.ts";
import { NODE_INDEX_MAX } from "../_catalog/workflow_spec.ts";
import { isArray, isObjectLike, isString } from "../codec/values.ts";
import type { NonEmpty } from "../wire/builders.ts";
import type { Kind, ObjectKind } from "../wire/kinds.ts";
import { err, ok, type Result } from "../wire/result.ts";
import type { ListedLookup } from "../send/send.ts";
import type { QueueSetup } from "../config/queues.ts";
import type { BuildRun, BuildSession, RecordedNode } from "./builder.ts";
import { nodeSlot } from "./builder.ts";
import type { WorkflowRecord } from "./define.ts";
import { type CheckedPolicy, type CheckedSpec, finding, type SpecFinding } from "./findings.ts";
import { compileContract } from "../contract/compile.ts";
import { storedFormFits } from "./kind-match.ts";
import { checkNodes, type NodeTarget, sourceOutputOf } from "./node-checks.ts";

/** The most nodes of one workflow (the core's `MAX_WORKFLOW_NODES`). */
export const MAX_WORKFLOW_NODES: number = NODE_INDEX_MAX + 1;

/** The longest node id, in characters. */
export const MAX_NODE_ID_LENGTH = 128;

/** A node id: letters, digits, `_`, `-`, `:` and `.`. */
const NODE_ID = /^[A-Za-z0-9_\-:.]+$/;

/** A workflow of the app's list: its record and the kind tree of its params. */
export type WorkflowLookup =
  | { readonly kind: "listed"; readonly record: WorkflowRecord; readonly params: ObjectKind; readonly output: Kind }
  | { readonly kind: "notListed" };

/** The facts of the app that the checks read. */
export interface SpecAppView {
  /** The listed task of a contract: its compiled form and its queue route. */
  task(contract: object): ListedLookup;
  /** The listed workflow of a workflow contract. */
  workflow(contract: object): WorkflowLookup;
  readonly queues: QueueSetup;
}

/** A build that passed the checks: the checked spec, and what each node runs, by node id. */
export type CheckedBuild = { readonly spec: CheckedSpec; readonly targets: ReadonlyMap<string, NodeTarget> };

/** The outcome of the checks of one build. */
export type BuildChecked = Result<CheckedBuild, NonEmpty<SpecFinding>>;

/** The parts of the returned object that gate 1 finds: the output node and the success policy, as node indexes. */
type Returned = { readonly output: number; readonly policy: CheckedPolicy };

/** A run of a build that returned a value that is no thenable. */
export type ReturnedRun = Extract<BuildRun, { readonly kind: "returned" }>;

/**
 * The checks of one build of `record`: the nodes that it made and the value that it returned. Pure: the checks
 * encode the static members, and run no user code but the checks of zod schemas.
 */
export function checkBuild(record: WorkflowRecord, run: ReturnedRun, view: SpecAppView): BuildChecked {
  const session = run.session;
  const nodes = session.nodes();
  const first = gateOne(record, session, nodes);
  const returnedParts = isObjectLike(run.returned) ? returnedOf(session, run.returned) : err([noOutput()]);

  if (!returnedParts.ok) {
    first.push(...returnedParts.error);
  }

  const [stop, ...more] = first;

  if (stop !== undefined || !returnedParts.ok) {
    return err(nonEmpty(stop, more));
  }

  const checked = checkNodes(nodes, session, view);
  const policy = policyFindings(returnedParts.value.policy);
  const output = outputFindings(record, nodes[returnedParts.value.output], view);
  const second = [...(checked.ok ? [] : checked.error), ...policy, ...output];
  const [found, ...rest] = second;

  if (found !== undefined || !checked.ok) {
    return err(nonEmpty(found, rest));
  }

  const onError = record.onError.kind === "valid" ? record.onError.onError : "FAIL";

  const spec: CheckedSpec = {
    name: record.name,
    definitionKey: record.definitionKey,
    onError,
    nodes: checked.value.nodes,
    output: returnedParts.value.output,
    successPolicy: returnedParts.value.policy,
  };

  return ok({ spec, targets: checked.value.targets });
}

function nonEmpty(first: SpecFinding | undefined, rest: readonly SpecFinding[]): NonEmpty<SpecFinding> {
  if (first === undefined) {
    throw new TypeError("checkBuild: a refused build with no finding");
  }

  return [first, ...rest];
}

/** The findings of gate 1 that the nodes give: the name, the count, the ids and the references. */
function gateOne(record: WorkflowRecord, session: BuildSession, nodes: readonly RecordedNode[]): SpecFinding[] {
  const findings: SpecFinding[] = [];

  if (record.name.trim() === "") {
    findings.push(finding(ErrorCode.WORKFLOW_NO_NAME, "name", "a workflow name must not be empty", []));
  }

  if (nodes.length === 0) {
    findings.push(finding(ErrorCode.WORKFLOW_NO_NODES, "nodes", "the build made no node", ["add at least one node"]));

    return findings;
  }

  if (nodes.length > MAX_WORKFLOW_NODES) {
    const detail = `the build made ${nodes.length} nodes, more than the limit of ${MAX_WORKFLOW_NODES}`;

    findings.push(
      finding(ErrorCode.WORKFLOW_TOO_MANY_NODES, "nodes", detail, [
        "split the nodes into sub-workflow nodes; each child workflow has its own limit",
      ]),
    );

    return findings;
  }

  findings.push(...idFindings(nodes), ...nodes.flatMap((node, index) => referenceFindings(session, node, index)));

  return findings;
}

/** The place of a node in a finding: `nodes[2] charge`; only `nodes[2]` for an id that breaks the id rule. Pure. */
export function nodePlace(index: number, node: RecordedNode): string {
  const id: unknown = node.options.id;

  return isValidNodeId(id) ? `nodes[${index}] ${id}` : `nodes[${index}]`;
}

/**
 * Whether a node id is a string of the id pattern with at most `MAX_NODE_ID_LENGTH` characters. The type gives a
 * string; untyped code or a cast can give another value, which is not an id. Pure.
 */
function isValidNodeId(id: unknown): id is string {
  return isString(id) && id.length <= MAX_NODE_ID_LENGTH && NODE_ID.test(id);
}

/** `HRS-003` for each id that breaks the id rule, and `HRS-004` for each valid id that an earlier node has. Pure. */
function idFindings(nodes: readonly RecordedNode[]): readonly SpecFinding[] {
  const findings: SpecFinding[] = [];
  const seen = new Map<string, number>();

  nodes.forEach((node, index) => {
    const id: unknown = node.options.id;

    if (!isValidNodeId(id)) {
      const detail = isString(id) ? `invalid node id ${JSON.stringify(id)}` : "invalid node id that is not a string";

      findings.push(
        finding(ErrorCode.WORKFLOW_INVALID_NODE_ID, `nodes[${index}]`, detail, [
          `a node id is a string that matches [A-Za-z0-9_\\-:.]+ and has at most ${MAX_NODE_ID_LENGTH} characters`,
        ]),
      );

      return;
    }

    const earlier = seen.get(id);

    if (earlier === undefined) {
      seen.set(id, index);
    } else {
      findings.push(
        finding(
          ErrorCode.WORKFLOW_DUPLICATE_NODE_ID,
          `nodes[${index}]`,
          `the node id "${id}" is also the id of nodes[${earlier}]`,
          [],
        ),
      );
    }
  });

  return findings;
}

/** `HRS-006` for each node that a node names and that is not a node of this build. */
function referenceFindings(session: BuildSession, node: RecordedNode, index: number): readonly SpecFinding[] {
  const named = [...inputNodes(node.input), ...(node.options.waitsFor ?? []), ...(node.options.ctxFrom ?? [])];

  return named.some((value) => !isNodeOf(session, value))
    ? [
        finding(
          ErrorCode.WORKFLOW_INVALID_DEPENDENCY,
          nodePlace(index, node),
          "the node names a node of another build",
          ["use the nodes that this build's w.task and w.subWorkflow returned"],
        ),
      ]
    : [];
}

/** The input members that are node references of any build. Pure. */
function inputNodes(input: object): readonly object[] {
  return Object.values(input).filter(
    (value): value is object => isObjectLike(value) && nodeSlot(value).kind === "node",
  );
}

/** Whether `value` is a node of `session`. Pure. */
function isNodeOf(session: BuildSession, value: object): boolean {
  const slot = nodeSlot(value);

  return slot.kind === "node" && slot.session === session;
}

/** `HRS-011`: the build returned no output node of its own. Pure. */
function noOutput(): SpecFinding {
  return finding(ErrorCode.WORKFLOW_INVALID_OUTPUT, "output", "the build returns no output node of this build", [
    "return { output: node } with a node that this build made",
  ]);
}

/** The index of a node of `session`, or none. Pure. */
function indexIn(session: BuildSession, value: object): number | "foreign" {
  const slot = nodeSlot(value);

  return slot.kind === "node" && slot.session === session ? slot.index : "foreign";
}

/** The output node and the success policy of the returned object (`HRS-011`, `HRS-012`). */
function returnedOf(session: BuildSession, returned: object): Result<Returned, readonly SpecFinding[]> {
  const output = "output" in returned ? returned.output : undefined;
  const index = isObjectLike(output) ? indexIn(session, output) : "foreign";

  if (index === "foreign") {
    return err([noOutput()]);
  }

  const policy = "successPolicy" in returned ? returned.successPolicy : undefined;

  if (policy === undefined) {
    return ok({ output: index, policy: { kind: "noPolicy" } });
  }

  const read = isObjectLike(policy) ? policyOf(session, policy) : "foreign";

  return read === "foreign"
    ? err([
        finding(
          ErrorCode.WORKFLOW_INVALID_SUCCESS_POLICY,
          "successPolicy",
          "the policy names a node of another build",
          ["name only nodes that this build made"],
        ),
      ])
    : ok({ output: index, policy: read });
}

/** The cases and the optional nodes as indexes, or `foreign` when one is not a node of this build. Pure. */
function policyOf(session: BuildSession, policy: object): CheckedPolicy | "foreign" {
  const cases = "cases" in policy && isArray(policy.cases) ? policy.cases : [];
  const optional = "optional" in policy && isArray(policy.optional) ? policy.optional : [];

  const caseIndexes = cases.map((given) => {
    const required = isObjectLike(given) && "required" in given && isArray(given.required) ? given.required : [];

    return allObjects(required) ? indexesIn(session, required) : "foreign";
  });

  const optionalIndexes = allObjects(optional) ? indexesIn(session, optional) : "foreign";

  if (optionalIndexes === "foreign" || caseIndexes.some((indexes) => indexes === "foreign")) {
    return "foreign";
  }

  return {
    kind: "cases",
    cases: caseIndexes.filter((indexes): indexes is readonly number[] => indexes !== "foreign"),
    optional: optionalIndexes,
  };
}

function indexesIn(session: BuildSession, values: readonly object[]): readonly number[] | "foreign" {
  const indexes: number[] = [];

  for (const value of values) {
    const index = indexIn(session, value);

    if (index === "foreign") {
      return "foreign";
    }

    indexes.push(index);
  }

  return indexes;
}

/** Whether every item of a list is an object. Pure. */
function allObjects(values: readonly unknown[]): values is readonly object[] {
  return values.every((value) => isObjectLike(value));
}

/**
 * `HRS-025` when the output node's type is not the workflow's declared output: their kinds do not have the same
 * stored form. An output node of a contract that the app does not list has a finding of its own. Pure.
 */
function outputFindings(
  record: WorkflowRecord,
  node: RecordedNode | undefined,
  view: SpecAppView,
): readonly SpecFinding[] {
  const declared = compileContract(record.paramsContract);
  const output = node === undefined ? { kind: "notKnown" as const } : sourceOutputOf(node, view);

  if (!declared.ok) {
    return [finding(ErrorCode.WORKFLOW_OUTPUT_TYPE_MISMATCH, "output", "the declared output does not compile", [])];
  }

  return output.kind === "known" && !storedFormFits(output.output, declared.value.output)
    ? [
        finding(
          ErrorCode.WORKFLOW_OUTPUT_TYPE_MISMATCH,
          "output",
          "the output node's type is not the declared output",
          ["return an output node whose output is the workflow's declared output"],
        ),
      ]
    : [];
}

/** `HRS-012` for a policy with no case, and for a case with no required node. */
function policyFindings(policy: CheckedPolicy): readonly SpecFinding[] {
  switch (policy.kind) {
    case "noPolicy":
      return [];
    case "cases":
      if (policy.cases.length === 0) {
        return [
          finding(ErrorCode.WORKFLOW_INVALID_SUCCESS_POLICY, "successPolicy.cases", "a policy has no case", [
            "add at least one case",
          ]),
        ];
      }

      return policy.cases.flatMap((required, position) =>
        required.length === 0
          ? [
              finding(
                ErrorCode.WORKFLOW_INVALID_SUCCESS_POLICY,
                `successPolicy.cases[${position}]`,
                "a case has no required node",
                ["add at least one node to required"],
              ),
            ]
          : [],
      );
    default: {
      const unexpected: never = policy;

      throw new TypeError(`policyFindings: a policy outside its type: ${String(unexpected)}`);
    }
  }
}
