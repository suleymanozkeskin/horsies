// The app project of the typecheck-speed check: the 500 tasks of the control project on the horsies API, from
// the same task model. 500 contracts (`defineTask`), 500 implementations (`app.implement`), 500 sends with a
// result read, 50 workflows of 10 nodes with 50 starts and a typed `resultFor`, and 20 schedules. The project
// imports the package by its name; its tsconfig maps the name to the source entry of this workspace.
//
// The project folder is a direct child of this folder, so the relative paths of the tsconfig are the same on
// every machine and the project hash does not depend on the checkout path.

import {
  assertNever,
  chunks,
  type ExecutionClass,
  type Field,
  fileOfTask,
  itemAt,
  type Kind,
  makeTasks,
  nodesPerWorkflow,
  pad,
  type PrimitiveKind,
  scheduleCount,
  scheduleTaskStep,
  sentFields,
  type Task,
  taskCount,
  tasksPerFile,
  valueOf,
  workflowFileCount,
  workflowsPerFile,
} from "./task-model.ts";

/** Every third workflow joins its last node with a quorum. 17 of 50 workflows. */
const quorumWorkflowStep = 3;

/** Every fifth workflow declares a success policy. 10 of 50 workflows. */
const successPolicyWorkflowStep = 5;

/** The node of each workflow that has an extra `waitsFor` edge. */
const waitingNode = 5;

/** The time bound of a task of a shared execution class, in milliseconds. */
const sharedTimeoutMilliseconds = 30_000;

const horsiesFolder = "../../../horsies";

const databaseSecret = '{ kind: "fromEnv", name: "DATABASE_URL" }';

function primitiveWire(kind: PrimitiveKind): string {
  switch (kind) {
    case "string":
      return "h.string()";
    case "int":
      return "h.int()";
    case "float":
      return "h.float()";
    case "bool":
      return "h.boolean()";
    case "uuid":
      return "h.uuid()";
    case "decimal":
      return "h.decimal()";
    case "instant":
      return "h.instant()";
    case "date":
      return "h.plainDate()";
    default:
      return assertNever(kind);
  }
}

/** The wire type of a kind, as source text. */
function wireOf(kind: Kind): string {
  switch (kind.kind) {
    case "enum":
      return `h.enum([${kind.members.map((member) => `"${member}"`).join(", ")}])`;
    case "array":
      return `h.array(${wireOf(kind.item)})`;
    case "object":
      return `h.object({ ${kind.fields.map(fieldWire).join(", ")} })`;
    case "string":
    case "int":
    case "float":
    case "bool":
    case "uuid":
    case "decimal":
    case "instant":
    case "date":
      return primitiveWire(kind.kind);
    default:
      return assertNever(kind);
  }
}

function fieldWire(field: Field): string {
  switch (field.presence) {
    case "required":
      return `${field.name}: ${wireOf(field.type)}`;
    case "optional":
      return `${field.name}: ${wireOf(field.type)}.optional()`;
    case "nullable":
      return `${field.name}: ${wireOf(field.type)}.nullable()`;
    default:
      return assertNever(field.presence);
  }
}

function executionOptions(execution: ExecutionClass): string {
  switch (execution) {
    case "ISOLATED":
      return "";
    case "SHARED_ASYNC":
    case "SHARED_THREAD":
      return `  execution: "${execution}",\n  timeoutMs: ${sharedTimeoutMilliseconds},\n`;
    default:
      return assertNever(execution);
  }
}

function retryOptions(task: Task): string {
  if (!task.retries) {
    return "";
  }

  const code = itemAt(task.codes, 0, "code");

  return `  retryPolicy: { kind: "exponential", baseSeconds: 2, maxRetries: 3, retryFor: ["${code}"] },\n`;
}

function codesOption(task: Task): string {
  return task.codes.length > 0 ? `  errorCodes: [${task.codes.map((code) => `"${code}"`).join(", ")}],\n` : "";
}

/** The input wires of a task: its parameters and, for a task with a predecessor, the optional outcome `prev`. */
function inputWires(task: Task): string {
  const params = task.params.map(fieldWire);
  const chain = task.previous.kind === "task" ? [`prev: h.taskOutcome(task${task.previous.index}).optional()`] : [];

  return [...params, ...chain].join(", ");
}

function contractSource(task: Task): string {
  return [
    `export const output${task.index} = ${wireOf(task.output)};`,
    "",
    `export const task${task.index} = defineTask({`,
    `  name: "task${task.index}",`,
    `  input: { ${inputWires(task)} },`,
    `  output: output${task.index},`,
    `${codesOption(task)}${executionOptions(task.execution)}${retryOptions(task)}});`,
  ].join("\n");
}

function contractsFile(group: readonly Task[]): string {
  return `import { defineTask, h } from "horsies";\n\n${group.map(contractSource).join("\n\n")}\n`;
}

function implementationBody(task: Task): string {
  if (task.codes.length > 0) {
    const firstParam = itemAt(task.params, 0, "parameter").name;
    const code = itemAt(task.codes, 0, "code");
    const upstream = task.previous.kind === "task" ? `  if (input.prev?.kind === "failed") return err({ code: "${code}", message: "upstream" });\n` : "";

    return `async (input, { ok, err }) => {\n  if (input.${firstParam} === undefined) return err({ code: "${code}", message: "missing" });\n${upstream}  return ok(${valueOf(task.output)});\n}`;
  }

  const chain = task.previous.kind === "task" ? "  void input.prev;\n" : "";

  return `async (input, { ok }) => {\n  void input;\n${chain}  return ok(${valueOf(task.output)});\n}`;
}

const valueImports = 'import { at, day, dec } from "../values.ts";\nvoid [at, day, dec];\n';

function implementationsFile(group: readonly Task[], file: number): string {
  const names = group.map((task) => `task${task.index}`).join(", ");
  const implementations = group.map((task) => `app.implement(task${task.index}, ${implementationBody(task)});`);

  return `import { app } from "../app.ts";\nimport { ${names} } from "../contracts/c${pad(file)}.ts";\n${valueImports}\n${implementations.join("\n\n")}\n`;
}

function sendsFile(group: readonly Task[], file: number): string {
  const names = group.map((task) => `task${task.index}`).join(", ");

  const sends = group.map(
    (task) =>
      `export async function send${task.index}(): Promise<string> {\n  const sent = await app.send(task${task.index}, { ${sentFields(task.params)} });\n  if (!sent.ok) return "refused";\n  const read = await sent.value.get({ waitMs: 1000 });\n  return read.ok ? read.value.kind : "unread";\n}`,
  );

  return `import { app } from "../app.ts";\nimport { ${names} } from "../contracts/c${pad(file)}.ts";\n${valueImports}\n${sends.join("\n\n")}\n`;
}

/** One import line per contract file that the tasks `indexes` and their outputs come from. */
function contractImports(indexes: readonly number[], withOutputs: boolean): string {
  return [...new Set(indexes.map(fileOfTask))]
    .map((file) => {
      const own = indexes.filter((index) => fileOfTask(index) === file);
      const names = own.flatMap((index) => (withOutputs ? [`task${index}`, `output${index}`] : [`task${index}`]));

      return `import { ${names.join(", ")} } from "../contracts/c${pad(file)}.ts";`;
    })
    .join("\n");
}

function nodeOptions(workflow: number, node: number): string {
  const id = `nodeId: "n${node}"`;
  const lastNode = nodesPerWorkflow - 1;

  if (node === lastNode && workflow % quorumWorkflowStep === 0) {
    return `{ ${id}, waitsFor: [n${node - 2}], join: { kind: "quorum", minSuccess: 1 } }`;
  }

  return node === waitingNode ? `{ ${id}, waitsFor: [n${node - 2}] }` : `{ ${id} }`;
}

function nodeSource(tasks: readonly Task[], workflow: number, node: number): string {
  const task = itemAt(tasks, workflow * nodesPerWorkflow + node, "task");
  const values = [sentFields(task.params), ...(node > 0 ? [`prev: n${node - 1}`] : [])].filter((text) => text !== "");

  return `    const n${node} = w.task(task${task.index}, { ${values.join(", ")} }, ${nodeOptions(workflow, node)});`;
}

function workflowReturn(workflow: number): string {
  const lastNode = nodesPerWorkflow - 1;

  if (workflow % successPolicyWorkflowStep === 0) {
    return `    return { output: n${lastNode}, successPolicy: { cases: [{ required: [n${lastNode}, n${waitingNode}] }], optional: [n0] } };`;
  }

  return `    return { output: n${lastNode} };`;
}

function workflowSource(tasks: readonly Task[], workflow: number): string {
  const lastTask = workflow * nodesPerWorkflow + nodesPerWorkflow - 1;
  const nodes = Array.from({ length: nodesPerWorkflow }, (_, node) => nodeSource(tasks, workflow, node));

  return [
    `export const workflow${workflow} = defineWorkflow(`,
    `  { name: "workflow${workflow}", definitionKey: "speed.workflow${workflow}.v1", outputType: output${lastTask}, parameters: {} },`,
    "  (w) => {",
    ...nodes,
    workflowReturn(workflow),
    "  },",
    ");",
  ].join("\n");
}

function workflowsFile(tasks: readonly Task[], file: number): string {
  const workflows = Array.from({ length: workflowsPerFile }, (_, position) => file * workflowsPerFile + position);
  const taskIndexes = Array.from({ length: workflowsPerFile * nodesPerWorkflow }, (_, position) => file * workflowsPerFile * nodesPerWorkflow + position);

  return `import { defineWorkflow } from "horsies";\n\n${contractImports(taskIndexes, true)}\n${valueImports}\n${workflows.map((workflow) => workflowSource(tasks, workflow)).join("\n\n")}\n`;
}

function startsFile(file: number): string {
  const workflows = Array.from({ length: workflowsPerFile }, (_, position) => file * workflowsPerFile + position);
  const names = workflows.map((workflow) => `workflow${workflow}`).join(", ");

  const starts = workflows.map(
    (workflow) =>
      `export async function start${workflow}(): Promise<string> {\n  const started = await app.start(workflow${workflow}, {});\n  if (!started.ok) return "refused";\n  const read = await started.value.resultFor("n${nodesPerWorkflow - 1}");\n  return read.ok ? read.value.kind : "unread";\n}`,
  );

  return `import { app } from "../app.ts";\nimport { ${names} } from "../workflows/w${pad(file)}.ts";\n\n${starts.join("\n\n")}\n`;
}

function appFile(): string {
  const taskNames = Array.from({ length: taskCount }, (_, index) => `task${index}`);
  const workflowCount = workflowFileCount * workflowsPerFile;
  const workflowNames = Array.from({ length: workflowCount }, (_, workflow) => `workflow${workflow}`);

  const taskImportLines = chunks(taskNames, tasksPerFile).map((names, file) => `import { ${names.join(", ")} } from "./contracts/c${pad(file)}.ts";`);
  const workflowImportLines = chunks(workflowNames, workflowsPerFile).map((names, file) => `import { ${names.join(", ")} } from "./workflows/w${pad(file)}.ts";`);

  return [
    'import { Horsies } from "horsies";',
    "",
    ...taskImportLines,
    ...workflowImportLines,
    "",
    "const created = Horsies.create({",
    `  database: { databaseUrl: ${databaseSecret} },`,
    '  workerProfiles: { default: { isolated: { kind: "isolatedChildren", count: 2 } } },',
    `  tasks: [${taskNames.join(", ")}],`,
    `  workflows: [${workflowNames.join(", ")}],`,
    "});",
    "",
    'if (!created.ok) throw new Error("the speed app is refused");',
    "",
    "export const app = created.value;",
    "",
  ].join("\n");
}

function schedulesFile(tasks: readonly Task[]): string {
  const scheduled = Array.from({ length: scheduleCount }, (_, position) => itemAt(tasks, position * scheduleTaskStep, "task"));

  const entries = scheduled.map(
    (task, position) =>
      `  taskSchedule({ name: "schedule-${position}", task: task${task.index}, input: { ${sentFields(task.params)} }, pattern: { kind: "interval", every: Temporal.Duration.from({ seconds: 10 }) } }),`,
  );

  return [
    'import { type ScheduleSection, taskSchedule } from "horsies";',
    contractImports(
      scheduled.map((task) => task.index),
      false,
    ).replaceAll('"../contracts/', '"./contracts/'),
    'import { at, day, dec } from "./values.ts";',
    "void [at, day, dec];",
    "",
    "export const SCHEDULE: ScheduleSection = { schedules: [",
    ...entries,
    "] };",
    "",
  ].join("\n");
}

const valuesFile = [
  'import type { Decimal } from "horsies";',
  "",
  "export declare const dec: Decimal;",
  "export declare const at: Temporal.Instant;",
  "export declare const day: Temporal.PlainDate;",
  "",
].join("\n");

const tsconfigFile = JSON.stringify(
  {
    extends: `${horsiesFolder}/tsconfig.json`,
    compilerOptions: {
      // An app publishes no declarations.
      isolatedDeclarations: false,
      declaration: false,
      paths: { horsies: [`${horsiesFolder}/src/index.ts`] },
    },
    include: ["**/*.ts"],
  },
  null,
  2,
);

/**
 * The files of the app project, by path relative to the project folder, `tsconfig.json` included. The folder must
 * be a direct child of the `typecheck-speed` folder: the tsconfig refers to the package by a relative path.
 */
export function appProjectFiles(): ReadonlyMap<string, string> {
  const tasks = makeTasks();
  const files = new Map<string, string>();

  chunks(tasks, tasksPerFile).forEach((group, file) => {
    files.set(`contracts/c${pad(file)}.ts`, contractsFile(group));
    files.set(`impls/i${pad(file)}.ts`, implementationsFile(group, file));
    files.set(`sends/s${pad(file)}.ts`, sendsFile(group, file));
  });

  for (let file = 0; file < workflowFileCount; file += 1) {
    files.set(`workflows/w${pad(file)}.ts`, workflowsFile(tasks, file));
    files.set(`starts/st${pad(file)}.ts`, startsFile(file));
  }

  files.set("app.ts", appFile());
  files.set("schedules.ts", schedulesFile(tasks));
  files.set("values.ts", valuesFile);
  files.set("tsconfig.json", tsconfigFile);

  return files;
}
