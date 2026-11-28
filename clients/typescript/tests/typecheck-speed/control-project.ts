// The control project of the typecheck-speed check: 500 plain typed async functions with
// the input and output types of 500 generated task contracts, 500 calls, 50 functions that
// each call 10 tasks in a chain, and 20 schedule entries. It uses no part of the horsies
// API. A fixed seed makes every call return the same files.

import {
  assertNever,
  chunks,
  fileOfTask,
  type Field,
  itemAt,
  type Kind,
  makeTasks,
  nodesPerWorkflow,
  pad,
  scheduleCount,
  scheduleTaskStep,
  sentFields,
  type Task,
  tasksPerFile,
  valueOf,
  workflowFileCount,
  workflowsPerFile,
} from "./task-model.ts";

function typeOf(kind: Kind): string {
  switch (kind.kind) {
    case "string":
    case "uuid":
      return "string";
    case "int":
    case "float":
      return "number";
    case "bool":
      return "boolean";
    case "decimal":
      return "Decimal";
    case "instant":
      return "Temporal.Instant";
    case "date":
      return "Temporal.PlainDate";
    case "enum":
      return kind.members.map((member) => `"${member}"`).join(" | ");
    case "array":
      return `(${typeOf(kind.item)})[]`;
    case "object":
      return `{ ${kind.fields.map(fieldType).join("; ")} }`;
    default:
      return assertNever(kind);
  }
}

function fieldType(field: Field): string {
  switch (field.presence) {
    case "required":
      return `${field.name}: ${typeOf(field.type)}`;
    case "optional":
      return `${field.name}?: ${typeOf(field.type)}`;
    case "nullable":
      return `${field.name}: ${typeOf(field.type)} | null`;
    default:
      return assertNever(field.presence);
  }
}

function taskImports(indexes: readonly number[], fromFolder: string): string {
  return [...new Set(indexes.map(fileOfTask))]
    .map((file) => {
      const names = indexes.filter((index) => fileOfTask(index) === file).map((index) => `task${index}`);

      return `import { ${names.join(", ")} } from "${fromFolder}k${pad(file)}";`;
    })
    .join("\n");
}

function typesFile(tasks: readonly Task[], group: readonly Task[]): string {
  const types = group.map((task) => {
    const prev = task.previous.kind === "task" ? [`prev?: Outcome<${typeOf(itemAt(tasks, task.previous.index, "task").output)}>`] : [];

    return [
      `export type Task${task.index}Input = { ${[...task.params.map(fieldType), ...prev].join("; ")} };`,
      `export type Task${task.index}Output = ${typeOf(task.output)};`,
    ].join("\n");
  });

  return `import type { Decimal, Outcome } from "../values";\n\n${types.join("\n")}\n`;
}

function tasksFile(group: readonly Task[], file: number): string {
  const bodies = group.map((task) => {
    const firstParam = itemAt(task.params, 0, "parameter").name;

    const check =
      task.codes.length > 0 ? `  if (input.${firstParam} === undefined) throw new Error("${itemAt(task.codes, 0, "code")}");\n` : "";

    const upstream = task.previous.kind === "task" ? `  if (input.prev?.kind === "failed") throw new Error("upstream");\n` : "";

    return `export async function task${task.index}(input: Task${task.index}Input): Promise<Task${task.index}Output> {\n${check}${upstream}  return ${valueOf(task.output)};\n}`;
  });

  const typeImports = group.flatMap((task) => [`Task${task.index}Input`, `Task${task.index}Output`]).join(", ");

  return `import type { ${typeImports} } from "../types/t${pad(file)}";\nimport { at, day, dec } from "../values";\nvoid [at, day, dec];\n\n${bodies.join("\n\n")}\n`;
}

function sendsFile(group: readonly Task[], file: number): string {
  const sends = group.map(
    (task) =>
      `export async function send${task.index}(): Promise<string> {\n  const value = await task${task.index}({ ${sentFields(task.params)} });\n  return typeof value;\n}`,
  );

  const names = group.map((task) => `task${task.index}`).join(", ");

  return `import { ${names} } from "../tasks/k${pad(file)}";\nimport { at, day, dec } from "../values";\nvoid [at, day, dec];\n\n${sends.join("\n\n")}\n`;
}

function workflowSource(tasks: readonly Task[], workflow: number): string {
  const steps = Array.from({ length: nodesPerWorkflow }, (_, node) => {
    const task = itemAt(tasks, workflow * nodesPerWorkflow + node, "task");
    const values = task.params.filter((field) => field.presence !== "optional").map((field) => `${field.name}: ${valueOf(field.type)}`);

    if (node > 0) {
      values.push(`prev: { kind: "succeeded", value: n${node - 1} }`);
    }

    return `  const n${node} = await task${task.index}({ ${values.join(", ")} });`;
  });

  return `export async function workflow${workflow}(region: string, count: number): Promise<unknown> {\n  void [region, count];\n${steps.join("\n")}\n  return n${nodesPerWorkflow - 1};\n}`;
}

function workflowsFile(tasks: readonly Task[], file: number): string {
  const workflows = Array.from({ length: workflowsPerFile }, (_, position) => file * workflowsPerFile + position);
  const flows = workflows.map((workflow) => workflowSource(tasks, workflow));
  const indexes = Array.from({ length: workflowsPerFile * nodesPerWorkflow }, (_, position) => file * workflowsPerFile * nodesPerWorkflow + position);

  return `${taskImports(indexes, "../tasks/")}\nimport { at, day, dec } from "../values";\nvoid [at, day, dec];\n\n${flows.join("\n\n")}\n`;
}

function schedulesFile(tasks: readonly Task[]): string {
  const scheduled = Array.from({ length: scheduleCount }, (_, position) => itemAt(tasks, position * scheduleTaskStep, "task"));

  return [
    taskImports(
      scheduled.map((task) => task.index),
      "./tasks/",
    ),
    `import { at, day, dec } from "./values";`,
    "void [at, day, dec];",
    "",
    "export const schedules: readonly { name: string; run: () => Promise<unknown> }[] = [",
    ...scheduled.map((task, position) => `  { name: "schedule-${position}", run: () => task${task.index}({ ${sentFields(task.params)} }) },`),
    "];",
    "",
  ].join("\n");
}

const valuesFile = [
  "export declare class Decimal {",
  "  private readonly decimalBrand: undefined;",
  "  readonly text: string;",
  "}",
  'export type Outcome<T> = { readonly kind: "succeeded"; readonly value: T } | { readonly kind: "failed"; readonly code: string };',
  "export declare const dec: Decimal;",
  "export declare const at: Temporal.Instant;",
  "export declare const day: Temporal.PlainDate;",
  "",
].join("\n");

const tsconfigFile = JSON.stringify(
  {
    compilerOptions: {
      target: "ES2023",
      lib: ["ESNext", "DOM"],
      module: "ESNext",
      moduleResolution: "bundler",
      strict: true,
      noEmit: true,
      skipLibCheck: true,
      types: [],
    },
    include: ["**/*.ts"],
  },
  null,
  2,
);

/** The files of the control project, by path relative to the project folder, `tsconfig.json` included. */
export function controlProjectFiles(): ReadonlyMap<string, string> {
  const tasks = makeTasks();
  const files = new Map<string, string>();
  chunks(tasks, tasksPerFile).forEach((group, file) => {
    files.set(`types/t${pad(file)}.ts`, typesFile(tasks, group));
    files.set(`tasks/k${pad(file)}.ts`, tasksFile(group, file));
    files.set(`sends/s${pad(file)}.ts`, sendsFile(group, file));
  });

  for (let file = 0; file < workflowFileCount; file += 1) {
    files.set(`workflows/w${pad(file)}.ts`, workflowsFile(tasks, file));
  }

  files.set("schedules.ts", schedulesFile(tasks));
  files.set("values.ts", valuesFile);
  files.set("tsconfig.json", tsconfigFile);

  return files;
}
