// The workflows of an app: each listed workflow with the kind tree of its params, the lookups by contract, by
// definition key and of a task by its stored name, and the app view of the spec checks.

import { encodeParametersWithReadBack } from "../codec/read-back.ts";
import { decodeParameters } from "../codec/reader.ts";
import type { CodecRefused } from "../codec/refusal.ts";
import { compileContract, type ContractRefused } from "../contract/compile.ts";
import type { QueueSetup } from "../config/queues.ts";
import type { ContractLookup, ListedLookup } from "../send/send.ts";
import { objectKind, type ObjectKind } from "../wire/kinds.ts";
import { isResultOfKind } from "../wire/result-of.ts";
import { err, ok, type Result } from "../wire/result.ts";
import type { SpecAppView, WorkflowLookup } from "./checks.ts";
import { type WorkflowRecord, workflowRecord } from "./define.ts";

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
  /** The listed task of a stored task name: the read form of a stored output node. */
  taskByName(taskName: string): ListedLookup;
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
    taskByName: (taskName: string): ListedLookup => tasks.byName(taskName),
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

  const decoded = decodeParameters(params, text.value, "worker");

  return decoded.ok ? ok(Object(decoded.value)) : decoded;
}
