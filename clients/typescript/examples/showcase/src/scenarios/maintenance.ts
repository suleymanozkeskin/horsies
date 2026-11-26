// `maintenance`: start the five maintenance workflows.

import type { AnyWorkflowContract, Result } from "horsies";

import { err, ok } from "../result.ts";
import { customerWinback } from "../workflows/customer-winback.ts";
import { fraudReview } from "../workflows/fraud-review.ts";
import { priceSync } from "../workflows/price-sync.ts";
import { seasonalMarkdown } from "../workflows/seasonal-markdown.ts";
import { warehouseTransfer } from "../workflows/warehouse-transfer.ts";
import { type ScenarioContext, sendErrorText, withScenario } from "./context.ts";
import { bullet, heading, say } from "./output.ts";

/** Start `workflow`, which takes no params, and print its id. */
async function startWorkflow(context: ScenarioContext, workflow: AnyWorkflowContract): Promise<Result<void, string>> {
  const started = await context.showcase.app.start(workflow, {});

  if (!started.ok) {
    return err(`start ${workflow.name}: ${sendErrorText(started.error)}`);
  }

  bullet(`${workflow.name}: ${started.value.workflowId}`);

  return ok(undefined);
}

/** The maintenance workflows, in their start order. */
const MAINTENANCE_WORKFLOWS: readonly AnyWorkflowContract[] = [
  priceSync,
  customerWinback,
  warehouseTransfer,
  seasonalMarkdown,
  fraudReview,
];

export async function runMaintenance(): Promise<Result<void, string>> {
  heading("Acme Clothing — maintenance");

  return withScenario(async (context) => {
    for (const workflow of MAINTENANCE_WORKFLOWS) {
      const started = await startWorkflow(context, workflow);

      if (!started.ok) {
        return started;
      }
    }

    say("maintenance workflows submitted");

    return ok(undefined);
  });
}
