// The app view of the spec checks over contract lists, and one build with its checks, for the workflow tests.

import assert from "node:assert/strict";

import { checkContractList, contractLookup } from "../../../horsies/src/app/contract-list.ts";
import type { QueueSetup } from "../../../horsies/src/config/queues.ts";
import { compileContract } from "../../../horsies/src/contract/compile.ts";
import { runBuild } from "../../../horsies/src/workflow/builder.ts";
import {
  type BuildChecked,
  checkBuild,
  type SpecAppView,
  type WorkflowLookup,
} from "../../../horsies/src/workflow/checks.ts";
import { workflowRecord } from "../../../horsies/src/workflow/define.ts";
import type { CheckedSpec, SpecFinding } from "../../../horsies/src/workflow/findings.ts";

/** The default queue mode. */
export const DEFAULT_QUEUES: QueueSetup = { kind: "default" };

/** The app view of `tasks` and `workflows` under `queues`. Every listed contract must compile. */
export function viewOf(
  tasks: readonly object[],
  workflows: readonly object[],
  queues: QueueSetup = DEFAULT_QUEUES,
): SpecAppView {
  const listed = checkContractList(tasks, queues);

  assert.deepEqual(listed.refusals, []);

  const lookup = contractLookup(listed.entries);
  const byWorkflow = new Map<object, WorkflowLookup>(workflows.map((workflow) => [workflow, listedWorkflow(workflow)]));

  return {
    task: (contract) => lookup.byContract(contract),
    workflow: (contract) => byWorkflow.get(contract) ?? { kind: "notListed" },
    queues,
  };
}

function listedWorkflow(workflow: object): WorkflowLookup {
  const record = workflowRecord(workflow);
  const compiled = compileContract(record.paramsContract);

  assert(compiled.ok, `the params of ${record.name} compile`);

  return { kind: "listed", record, params: compiled.value.parameters, output: compiled.value.output };
}

/** The checks of one build of `workflow` with `params`. The build must return. */
export function checked(workflow: object, params: object, view: SpecAppView): BuildChecked {
  const record = workflowRecord(workflow);
  const run = runBuild(record.build, params);

  assert.equal(run.kind, "returned");

  return run.kind === "returned" ? checkBuild(record, run, view) : assert.fail("the build did not return");
}

/** The checked spec of a build that passes. */
export function passedSpec(workflow: object, params: object, view: SpecAppView): CheckedSpec {
  const outcome = checked(workflow, params, view);

  assert(outcome.ok, outcome.ok ? "" : findingsText(outcome.error));

  return outcome.value.spec;
}

/** The codes of the findings of a build that fails, in order. */
export function findingCodes(workflow: object, params: object, view: SpecAppView): readonly string[] {
  const outcome = checked(workflow, params, view);

  assert(!outcome.ok, "the build has a finding");

  return outcome.error.map((found) => found.code);
}

/** The findings as text, for a failed assertion. */
export function findingsText(findings: readonly SpecFinding[]): string {
  return findings.map((found) => `${found.code} ${found.place}: ${found.detail}`).join("; ");
}
