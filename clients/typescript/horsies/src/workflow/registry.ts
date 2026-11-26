// The workflows of an app: each listed workflow with the kind tree of its params, the lookups by contract and by
// definition key, the app view of the spec checks, and the output form of a workflow for its reads.

import { encodeParametersWithReadBack } from "../codec/read-back.ts";
import { decodeParameters } from "../codec/reader.ts";
import type { CodecRefused } from "../codec/refusal.ts";
import { compileContract, type ContractRefused } from "../contract/compile.ts";
import type { QueueSetup } from "../config/queues.ts";
import { type ReadForm, readFormOf } from "../read/handle.ts";
import type { ContractLookup } from "../send/send.ts";
import type { DeclaredCodes } from "../contract/definition.ts";
import { type Kind, objectKind, type ObjectKind } from "../wire/kinds.ts";
import { isTaskOutcomeKind } from "../wire/task-outcome.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import type { SpecAppView, WorkflowLookup } from "./checks.ts";
import { type WorkflowRecord, workflowRecord } from "./define.ts";
import type { NodeTarget } from "./node-checks.ts";

/** The deepest child workflow (the core's `MAX_SUBWORKFLOW_DEPTH`). */
export const MAX_SUBWORKFLOW_DEPTH = 32;

/** A workflow of the app's list: its contract, its record, the kind tree of its params and its declared output. */
export type ListedWorkflow = {
  readonly contract: object;
  readonly record: WorkflowRecord;
  readonly params: ObjectKind;
  /** The declared output kind: the read form of every result of the workflow. */
  readonly output: Kind;
};

/** The codes of a workflow's result: open, since a workflow holds the codes of many tasks. */
const OPEN_CODES: DeclaredCodes = Object.freeze({ kind: "open" });

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

  return compiled.ok
    ? ok(Object.freeze({ contract, record, params: compiled.value.parameters, output: compiled.value.output }))
    : err(compiled.error);
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
  return Object.freeze({ kind: "listed", record: listed.record, params: listed.params, output: listed.output });
}

/**
 * The params value that a build gets for `given`: encoded with the plain parameters, read back, then decoded in the
 * worker form with every parameter (defaults filled). The same value that a child build gets from the stored text.
 * Pure.
 */
export function buildParams(params: ObjectKind, given: object): Result<object, CodecRefused> {
  const plain = objectKind(params.members.filter((member) => !isTaskOutcomeKind(member.kind)));
  const text = encodeParametersWithReadBack(plain, given);

  if (!text.ok) {
    return text;
  }

  const decoded = decodeParameters(params, text.value, "worker");

  return decoded.ok ? ok(Object(decoded.value)) : decoded;
}

/** The read form of a stored output, or the name of a workflow that the app does not list (it has no form). */
export type OutputReadForm =
  | { readonly kind: "form"; readonly form: ReadForm }
  | { readonly kind: "workflowNotListed"; readonly workflowName: string };

/**
 * The read form of a workflow's output: the output kind that the workflow declares (`defineWorkflow({ output })`),
 * with open custom codes. The checks refuse every build whose output node does not fit it (`HRS-025`), so the declared
 * kind is the one fact; no build runs. A workflow that the app does not list is `workflowNotListed`. Pure.
 */
export function workflowOutputForm(workflow: object, registry: WorkflowRegistry): OutputReadForm {
  const listed = registry.byContract(workflow);

  switch (listed.kind) {
    case "listed":
      return Object.freeze({
        kind: "form",
        form: Object.freeze({ kind: "readable", output: listed.output, codes: OPEN_CODES }),
      });
    case "notListed":
      return Object.freeze({ kind: "workflowNotListed", workflowName: workflowRecord(workflow).name });
    default:
      return unreachable(listed, "workflowOutputForm");
  }
}

/**
 * The read form of a workflow's declared output from its own contract, with open custom codes: the app need not list
 * the workflow. Effects: compiles the params contract at its first use (see `compileContract`).
 */
export function declaredOutputForm(workflow: object): ReadForm {
  const compiled = compileContract(workflowRecord(workflow).paramsContract);

  if (compiled.ok) {
    return Object.freeze({ kind: "readable", output: compiled.value.output, codes: OPEN_CODES });
  }

  const causes = compiled.error.map((refused) => `${refused.path}: ${refused.detail}`).join("; ");

  return Object.freeze({ kind: "unreadable", cause: `the workflow contract is refused: ${causes}` });
}

/** The read form of what a node runs: its task's contract, or its child's declared output with open codes. */
export function nodeTargetForm(target: NodeTarget, registry: WorkflowRegistry): OutputReadForm {
  switch (target.kind) {
    case "task":
      return Object.freeze({ kind: "form", form: readFormOf(target.contract) });
    case "subWorkflow":
      return workflowOutputForm(target.workflow, registry);
    default:
      return unreachable(target, "nodeTargetForm");
  }
}
