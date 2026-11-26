// The checks of the `check` command for the app's workflows: each workflow with no params and each check case is
// built and checked, and its sub-workflows are walked down to the depth limit. A nesting is reported only when it
// fails for certain: a child that includes itself with the same params and no wait between (`HRS-037`), and a nesting
// past depth 32 with no wait on the path (`HRS-036`). The checks of the Rust client's `check_findings`.

import { HorsiesErrorCode } from "../_catalog/error_codes.ts";
import { refusal, type Refusal } from "../app/refusal.ts";
import { decodeParameters } from "../codec/reader.ts";
import { refusalText } from "../codec/refusal.ts";
import { thrownFacts } from "../native/call/thrown-facts.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import { runBuild } from "./builder.ts";
import { checkBuild } from "./checks.ts";
import { findingRefusal } from "./create-checks.ts";
import type { WorkflowRecord } from "./define.ts";
import type { CheckedSpec } from "./findings.ts";
import { checkCaseParams } from "./check-case.ts";
import { type ListedWorkflow, MAX_SUBWORKFLOW_DEPTH, type WorkflowRegistry } from "./registry.ts";

/** How the engine starts the child of a sub-workflow node: with its workflow, or after the node's dependencies. */
type ChildStart = "withItsWorkflow" | "afterDependencies";

/** One sub-workflow node on the path from a root build. */
type NestingStep = { readonly definitionKey: string; readonly paramsText: string; readonly start: ChildStart };

/** The refusals of every listed workflow, in list order. Effects: the builds of the workflows and their children. */
export function checkWorkflows(registry: WorkflowRegistry): readonly Refusal[] {
  return registry.workflows.flatMap((entry) => workflowRefusals(registry, entry));
}

function workflowRefusals(registry: WorkflowRegistry, entry: ListedWorkflow): readonly Refusal[] {
  const path = `workflow ${entry.record.name}`;

  if (entry.record.paramNames.length === 0) {
    return rootRefusals(registry, entry, {}, path);
  }

  return entry.record.checkCases.flatMap((given, position) => {
    const casePath = `${path} checkCases[${position}]`;
    const params = checkCaseParams(entry.params, given);

    return params.ok
      ? rootRefusals(registry, entry, params.value, casePath)
      : [
          refusal(HorsiesErrorCode.WORKFLOW_CHECK_CASE_INVALID, casePath, refusalText(params.error), [
            "give each case the params of the workflow",
          ]),
        ];
  });
}

/** The refusals of one root build, and of the walk into its children. */
function rootRefusals(
  registry: WorkflowRegistry,
  entry: ListedWorkflow,
  params: object,
  path: string,
): readonly Refusal[] {
  const built = checkedSpec(registry, entry.record, params, path);

  return built.ok ? walk(registry, built.value, [], path) : built.error;
}

/** The checked spec of one build, or its refusals (`HRS-029` for a throw or a thenable, then the spec findings). */
function checkedSpec(
  registry: WorkflowRegistry,
  record: WorkflowRecord,
  params: object,
  path: string,
): Result<CheckedSpec, readonly Refusal[]> {
  const run = runBuild(record.build, params);

  switch (run.kind) {
    case "threw": {
      const facts = thrownFacts(run.cause);

      return err([
        refusal(
          HorsiesErrorCode.WORKFLOW_CHECK_BUILDER_EXCEPTION,
          path,
          `the build threw ${facts.name}: ${facts.message}`,
          [],
        ),
      ]);
    }

    case "thenable":
      return err([
        refusal(HorsiesErrorCode.WORKFLOW_CHECK_BUILDER_EXCEPTION, path, "the build returned a thenable; a build is sync", []),
      ]);
    case "returned": {
      const checked = checkBuild(record, run, registry.view);

      return checked.ok ? ok(checked.value.spec) : err(checked.error.map((found) => findingRefusal(path, found)));
    }

    default:
      return unreachable(run, "checkedSpec");
  }
}

/** Walk the sub-workflow nodes of `spec`, reached by `path`, into the children that the check can build. */
function walk(
  registry: WorkflowRegistry,
  spec: CheckedSpec,
  steps: readonly NestingStep[],
  path: string,
): readonly Refusal[] {
  const refusals: Refusal[] = [];

  for (const node of spec.nodes) {
    // A node with args_from takes params from results at run time: its child is not built.
    if (node.body.kind !== "subWorkflow" || node.argsFrom.size > 0) {
      continue;
    }

    const child = registry.byKey(node.body.definitionKey);

    if (child.kind === "notListed") {
      continue;
    }

    const step: NestingStep = {
      definitionKey: node.body.definitionKey,
      paramsText: node.staticInput,
      start: node.dependencies.length === 0 ? "withItsWorkflow" : "afterDependencies",
    };

    const nodePath = `${path} > ${node.nodeId}`;
    const decision = decideNesting(steps, step, nodePath);

    switch (decision.kind) {
      case "stop":
        refusals.push(...decision.refusals);
        break;
      case "walk": {
        const params = decodeParameters(child.params, node.staticInput, "worker");

        if (!params.ok) {
          // The node checks read the params back, so only a check of the params that is not pure gets here. The
          // Rust client's walk gives the same finding.
          refusals.push(
            refusal(
              HorsiesErrorCode.WORKFLOW_KWARGS_NOT_SERIALIZABLE,
              nodePath,
              `the params of the sub-workflow node do not decode: ${refusalText(params.error)}`,
              ["make each check of the child's params pure"],
            ),
          );

          break;
        }

        const built = checkedSpec(registry, child.record, Object(params.value), nodePath);

        refusals.push(...(built.ok ? walk(registry, built.value, decision.steps, nodePath) : built.error));

        break;
      }

      default:
        unreachable(decision, "walk");
    }
  }

  return refusals;
}

type NestingDecision =
  | { readonly kind: "walk"; readonly steps: readonly NestingStep[] }
  | { readonly kind: "stop"; readonly refusals: readonly Refusal[] };

/**
 * The decision for the child of one node (the Rust client's `decide_nesting`): the same definition with the same
 * params on the path stops the walk, with `HRS-037` when no node between the two waits; a child past the depth limit
 * stops it, with `HRS-036` when no node on the path waits. Pure.
 */
function decideNesting(steps: readonly NestingStep[], step: NestingStep, path: string): NestingDecision {
  const walked = [...steps, step];

  const earlier = steps.findIndex(
    (known) => known.definitionKey === step.definitionKey && known.paramsText === step.paramsText,
  );

  if (earlier >= 0) {
    return {
      kind: "stop",
      refusals: startsForCertain(walked.slice(earlier + 1))
        ? [
            refusal(
              HorsiesErrorCode.WORKFLOW_SUBWORKFLOW_SELF_NESTING,
              path,
              "a sub-workflow includes itself with the same params, and no node between the two waits",
              ["change the params at each level, so that the nesting ends"],
            ),
          ]
        : [],
    };
  }

  if (steps.length >= MAX_SUBWORKFLOW_DEPTH) {
    return {
      kind: "stop",
      refusals: startsForCertain(walked)
        ? [
            refusal(
              HorsiesErrorCode.WORKFLOW_SUBWORKFLOW_DEPTH_LIMIT,
              path,
              `the child workflow would have depth ${walked.length}; the limit is ${MAX_SUBWORKFLOW_DEPTH}`,
              ["nest fewer levels; at run time this node fails with SUBWORKFLOW_DEPTH_LIMIT_REACHED"],
            ),
          ]
        : [],
    };
  }

  return { kind: "walk", steps: walked };
}

/** Whether the engine starts the child of each step with no wait. Pure. */
function startsForCertain(steps: readonly NestingStep[]): boolean {
  return steps.every((step) => step.start === "withItsWorkflow");
}
