// The workflows of an app: each listed workflow with the kind tree of its params, the lookups by contract and by
// definition key, the app view of the spec checks, and the output form of a workflow for its reads.

import { jsonTextCheckedByCore } from "../codec/json-text.ts";
import { encodeParametersWithReadBack } from "../codec/read-back.ts";
import { decodeParameters } from "../codec/reader.ts";
import { type CodecRefused, refusalText } from "../codec/refusal.ts";
import { compileContract, type ContractRefused } from "../contract/compile.ts";
import type { QueueSetup } from "../config/queues.ts";
import { type ReadForm, readFormOf } from "../read/handle.ts";
import type { ContractLookup } from "../send/send.ts";
import { objectKind, type ObjectKind } from "../wire/kinds.ts";
import { isResultOfKind } from "../wire/result-of.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import { runBuild } from "./builder.ts";
import { checkBuild, type SpecAppView, type WorkflowLookup } from "./checks.ts";
import { type WorkflowRecord, workflowRecord } from "./define.ts";
import type { NodeTarget } from "./node-checks.ts";

/** The deepest child workflow (the core's `MAX_SUBWORKFLOW_DEPTH`). */
export const MAX_SUBWORKFLOW_DEPTH = 32;

/** A workflow of the app's list: its contract, its record and the kind tree of its params. */
export type ListedWorkflow = {
  readonly contract: object;
  readonly record: WorkflowRecord;
  readonly params: ObjectKind;
};

/** The workflows of an app. */
export interface WorkflowRegistry {
  /** The listed workflow of a contract object. */
  byContract(contract: object): WorkflowLookup;
  /** The listed workflow of a definition key: the child of a sub-workflow node. */
  byKey(definitionKey: string): WorkflowLookup;
  /** The app view of the spec checks. */
  readonly view: SpecAppView;
  /** The listed workflows, in list order. */
  readonly workflows: readonly ListedWorkflow[];
}

const NOT_LISTED: WorkflowLookup = Object.freeze({ kind: "notListed" });

/** The listed form of a workflow contract: its params compiled, or every refusal of them. */
export function listedWorkflowOf(contract: object): Result<ListedWorkflow, readonly ContractRefused[]> {
  const record = workflowRecord(contract);
  const compiled = compileContract(record.paramsContract);

  return compiled.ok ? ok(Object.freeze({ contract, record, params: compiled.value.parameters })) : err(compiled.error);
}

/** The registry of `workflows`, with the app's tasks and queues. Pure. */
export function workflowRegistry(
  workflows: readonly ListedWorkflow[],
  tasks: ContractLookup,
  queues: QueueSetup,
): WorkflowRegistry {
  const byContract = new Map(workflows.map((listed) => [listed.contract, lookupOf(listed)]));
  const byKey = new Map(workflows.map((listed) => [listed.record.definitionKey, lookupOf(listed)]));
  const lookupContract = (contract: object): WorkflowLookup => byContract.get(contract) ?? NOT_LISTED;

  const view: SpecAppView = Object.freeze({
    task: (contract: object) => tasks.byContract(contract),
    workflow: lookupContract,
    queues,
  });

  return Object.freeze({
    byContract: lookupContract,
    byKey: (definitionKey: string): WorkflowLookup => byKey.get(definitionKey) ?? NOT_LISTED,
    view,
    workflows: Object.freeze([...workflows]),
  });
}

function lookupOf(listed: ListedWorkflow): WorkflowLookup {
  return Object.freeze({ kind: "listed", record: listed.record, params: listed.params });
}

/**
 * The params value that a build gets for `given`: encoded with the plain parameters, read back, then decoded in the
 * worker form with every parameter (defaults filled). The same value that a child build gets from the stored text.
 * Pure.
 */
export function buildParams(params: ObjectKind, given: object): Result<object, CodecRefused> {
  const plain = objectKind(params.members.filter((member) => !isResultOfKind(member.kind)));
  const text = encodeParametersWithReadBack(plain, given);

  if (!text.ok) {
    return text;
  }

  const decoded = decodeParameters(params, jsonTextCheckedByCore(text.value), "worker");

  return decoded.ok ? ok(Object(decoded.value)) : decoded;
}

/** The output forms of the workflows of each registry, kept per workflow contract. */
const outputForms = new WeakMap<WorkflowRegistry, WeakMap<object, ReadForm>>();

function formsOf(registry: WorkflowRegistry): WeakMap<object, ReadForm> {
  const kept = outputForms.get(registry);

  if (kept !== undefined) {
    return kept;
  }

  const forms = new WeakMap<object, ReadForm>();

  outputForms.set(registry, forms);

  return forms;
}

/**
 * The read form of a workflow's output: the output kind of its output node, with open user codes. The output node is
 * found by one build with no params or with the first check case; a sub-workflow output node gives its child's form,
 * at most `MAX_SUBWORKFLOW_DEPTH` levels down. The type of a build's return fixes the output node's type, so any build
 * that passes its checks gives the same form. Kept per contract. Effects: the user's build.
 */
export function workflowOutputForm(workflow: object, registry: WorkflowRegistry): ReadForm {
  return outputFormAt(workflow, registry, 0);
}

function outputFormAt(workflow: object, registry: WorkflowRegistry, depth: number): ReadForm {
  const forms = formsOf(registry);
  const kept = forms.get(workflow);

  if (kept !== undefined) {
    return kept;
  }

  const form =
    depth > MAX_SUBWORKFLOW_DEPTH
      ? unreadable("the child workflows nest deeper than 32 levels")
      : builtOutputForm(workflow, registry, depth);

  forms.set(workflow, form);

  return form;
}

function builtOutputForm(workflow: object, registry: WorkflowRegistry, depth: number): ReadForm {
  const listed = registry.byContract(workflow);

  if (listed.kind === "notListed") {
    return unreadable("the workflow is not in the workflows list of the app");
  }

  const given = listed.record.paramNames.length === 0 ? {} : listed.record.checkCases[0];

  if (given === undefined) {
    return unreadable("the workflow has no check case to build its output form with");
  }

  const params = buildParams(listed.params, given);

  if (!params.ok) {
    return unreadable(refusalText(params.error));
  }

  const run = runBuild(listed.record.build, params.value);

  if (run.kind !== "returned") {
    return unreadable("the build to find the output node did not return a value");
  }

  const checked = checkBuild(listed.record, run, registry.view);

  if (!checked.ok) {
    return unreadable(`the build to find the output node fails its checks (${checked.error[0].code})`);
  }

  const output = checked.value.spec.nodes[checked.value.spec.output];
  const target = output === undefined ? undefined : checked.value.targets.get(output.nodeId);

  return target === undefined ? unreadable("the output node has no target") : targetFormAt(target, registry, depth + 1);
}

/** The read form of what a node runs: its task's contract, or its child's output form with open codes. */
export function nodeTargetForm(target: NodeTarget, registry: WorkflowRegistry): ReadForm {
  return targetFormAt(target, registry, 0);
}

function targetFormAt(target: NodeTarget, registry: WorkflowRegistry, depth: number): ReadForm {
  switch (target.kind) {
    case "task":
      return readFormOf(target.contract);
    case "subWorkflow":
      return outputFormAt(target.workflow, registry, depth);
    default:
      return unreachable(target, "targetFormAt");
  }
}

function unreadable(cause: string): ReadForm {
  return Object.freeze({ kind: "unreadable", cause });
}
