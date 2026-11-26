// A task that starts a workflow at run time: the region of the report is the task's input, and the workflow's build
// uses it. The task does not wait for the workflow (a wait inside a task is refused); it returns the workflow id, and
// the demo reads the workflow's result by that id.
//
//   node examples/dynamic-start.ts worker --profile default
//   node examples/dynamic-start.ts demo

import { defineTask, defineWorkflow, h, Horsies, type Result, WorkflowId } from "horsies";

import {
  DATABASE_URL,
  DEFAULT_PROFILE,
  DEMO_DONE,
  demoFailed,
  failureText,
  ISOLATED_PROFILE,
  LONG_RESULT_WAIT_MS,
  outcomeLine,
  printLine,
  readErrorText,
  refusalLines,
  runExample,
  sendErrorText,
  workflowReadErrorText,
} from "./support.ts";

/** The sales of a region, in cents: 1,000 per character of its name. */
const SALES_CENTS_PER_CHARACTER = 1_000;

const fetchSales = defineTask({ name: "fetch_sales", input: { region: h.string() }, output: h.int() });

/** A node's dependency result arrives as an outcome: the parameter is declared `h.taskOutcome(fetchSales)`. */
const formatReport = defineTask({ name: "format_report", input: { sales: h.taskOutcome(fetchSales) }, output: h.string() });

/** The report of one region. `check` builds it with the check case. */
const regionalReport = defineWorkflow(
  {
    name: "regional_report",
    definitionKey: "examples.regional_report.v1",
    parameters: { region: h.string() },
    outputType: h.string(),
    checkCases: [{ region: "emea" }],
  },
  (w, { region }) => {
    const sales = w.task(fetchSales, { region }, { nodeId: "fetch_sales" });
    const report = w.task(formatReport, { sales }, { nodeId: "format_report" });

    return { output: report };
  },
);

/** Start the report of a region and give its workflow id. */
const startReport = defineTask({
  name: "start_report",
  input: { region: h.string() },
  output: h.uuid(),
  errorCodes: ["SPEC_REFUSED", "START_FAILED"],
});

const created = Horsies.create({
  broker: { databaseUrl: DATABASE_URL },
  workerProfiles: { [DEFAULT_PROFILE]: ISOLATED_PROFILE },
  tasks: [fetchSales, formatReport, startReport],
  workflows: [regionalReport],
});

if (!created.ok) {
  process.stderr.write(`${refusalLines(created.error.refusals)}\n`);
  process.exit(1);
}

const app = created.value;

app.implement(fetchSales, async ({ region }, { ok }) => ok(region.length * SALES_CENTS_PER_CHARACTER));

app.implement(formatReport, async ({ sales }, { ok }) => {
  switch (sales.kind) {
    case "succeeded":
      return ok(`sales: ${String(sales.value)} cents`);
    case "failed":
      return ok(`no sales figure: ${failureText(sales.error)}`);
    default: {
      const unexpected: never = sales;

      throw new TypeError(`format_report: an outcome outside its type: ${String(unexpected)}`);
    }
  }
});

// A task body starts the workflow through the app, as a producer does.
app.implement(startReport, async ({ region }, { ok, err }) => {
  const started = await app.start(regionalReport, { region });

  if (started.ok) {
    return ok(started.value.workflowId);
  }

  switch (started.error.code) {
    case "VALIDATION_FAILED":
      return err({ code: "SPEC_REFUSED", message: sendErrorText(started.error) });
    case "WORKFLOW_ID_CONFLICT":
    case "WORKFLOW_COMMAND_CONFLICT":
    case "ENQUEUE_FAILED":
    case "SEAT_RELAY_FULL":
      return err({ code: "START_FAILED", message: sendErrorText(started.error) });
    default: {
      const unexpected: never = started.error;

      throw new TypeError(`start_report: a start error outside its type: ${String(unexpected)}`);
    }
  }
});

async function demo(): Promise<Result<void, string>> {
  const sent = await app.send(startReport, { region: "emea" });

  if (!sent.ok) {
    return demoFailed(`send start_report: ${sendErrorText(sent.error)}`);
  }

  const read = await sent.value.get({ waitMs: LONG_RESULT_WAIT_MS });

  if (!read.ok) {
    return demoFailed(`read start_report: ${readErrorText(read.error)}`);
  }

  if (read.value.kind === "failed") {
    printLine(`start_report failed: ${failureText(read.value.error)}`);

    return DEMO_DONE;
  }

  const workflowId = WorkflowId.parse(read.value.value);

  if (!workflowId.ok) {
    return demoFailed(`start_report gave ${read.value.value}, which is not a workflow id: ${workflowId.error.detail}`);
  }

  printLine(`start_report started workflow ${workflowId.value}`);

  const report = await app.workflowHandle(regionalReport, workflowId.value).get({ waitMs: LONG_RESULT_WAIT_MS });

  if (!report.ok) {
    return demoFailed(`read regional_report: ${workflowReadErrorText(report.error)}`);
  }

  printLine(outcomeLine("regional_report", report.value, (text) => text));

  return DEMO_DONE;
}

process.exitCode = await runExample(app, demo);
