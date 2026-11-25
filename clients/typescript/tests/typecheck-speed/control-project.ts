// The control project of the typecheck-speed check: 500 plain typed async functions with
// the input and output types of 500 generated task contracts, 500 calls, 50 functions that
// each call 10 tasks in a chain, and 20 schedule entries. It uses no part of the horsies
// API. A fixed seed makes every call return the same files.

const generatorSeed = 20261125;
const taskCount = 500;
const tasksPerFile = 10;
const nodesPerWorkflow = 10;
const workflowsPerFile = 10;
const workflowFileCount = 5;
const scheduleCount = 20;
const scheduleTaskStep = 7;

/** A field type nests objects and arrays to at most this depth below a task parameter. */
const maxNestingDepth = 2;

const primitiveKinds = ["string", "int", "float", "bool", "uuid", "decimal", "instant", "date"] as const;
const enumMembers = ["red", "green", "blue"] as const;

type PrimitiveKind = (typeof primitiveKinds)[number];

/** The type of one generated field. Its depth is at most `maxNestingDepth + 1`. */
type Kind =
  | { readonly kind: PrimitiveKind }
  | { readonly kind: "enum"; readonly members: readonly string[] }
  | { readonly kind: "array"; readonly item: Kind }
  | { readonly kind: "object"; readonly fields: readonly Field[] };

interface Field {
  readonly name: string;
  readonly type: Kind;
  readonly presence: "required" | "optional" | "nullable";
}

/** The task before this one in its workflow chain: the task takes that task's outcome as `prev`. */
type PreviousTask = { readonly kind: "none" } | { readonly kind: "task"; readonly index: number };

interface Task {
  readonly index: number;
  readonly params: readonly Field[];
  readonly output: Kind;
  readonly codes: readonly string[];
  readonly previous: PreviousTask;
}

/** A fraction in [0, 1) from the generator's sequence. */
type Random = () => number;

function assertNever(value: never): never {
  throw new Error(`control project: unexpected variant ${JSON.stringify(value)}`);
}

/** mulberry32: the generator of the type-level spike, so the project equals the spike's control project. */
function createRandom(seed: number): Random {
  let state = seed;

  return () => {
    state = (state + 0x6d2b79f5) | 0;
    let mixed = state;
    mixed = Math.imul(mixed ^ (mixed >>> 15), mixed | 1);
    mixed ^= mixed + Math.imul(mixed ^ (mixed >>> 7), mixed | 61);

    return ((mixed ^ (mixed >>> 14)) >>> 0) / 4294967296;
  };
}

function pick<T>(random: Random, items: readonly T[]): T {
  const item = items[Math.floor(random() * items.length)];

  if (item === undefined) {
    throw new Error("control project: pick from an empty list");
  }

  return item;
}

function between(random: Random, low: number, high: number): number {
  return low + Math.floor(random() * (high - low + 1));
}

function itemAt<T>(items: readonly T[], index: number, subject: string): T {
  const item = items[index];

  if (item === undefined) {
    throw new Error(`control project: no ${subject} at index ${index}`);
  }

  return item;
}

function drawKindClass(roll: number, depth: number): "object" | "array" | "enum" | "primitive" {
  const nested = depth < maxNestingDepth;

  if (nested && roll < 0.18) {
    return "object";
  }

  if (nested && roll < 0.28) {
    return "array";
  }

  return roll < 0.36 ? "enum" : "primitive";
}

function makeKind(random: Random, depth: number): Kind {
  const kindClass = drawKindClass(random(), depth);

  switch (kindClass) {
    case "object":
      return { kind: "object", fields: makeFields(random, between(random, 2, 4), depth + 1) };
    case "array":
      return { kind: "array", item: makeKind(random, depth + 1) };
    case "enum":
      return { kind: "enum", members: enumMembers.slice(0, between(random, 2, 3)) };
    case "primitive":
      return { kind: pick(random, primitiveKinds) };
    default:
      return assertNever(kindClass);
  }
}

function drawPresence(random: Random): Field["presence"] {
  if (random() < 0.15) {
    return "optional";
  }

  return random() < 0.1 ? "nullable" : "required";
}

function makeFields(random: Random, count: number, depth: number): Field[] {
  return Array.from({ length: count }, (_, index) => ({
    name: `f${index}`,
    type: makeKind(random, depth),
    presence: drawPresence(random),
  }));
}

function makeOutput(random: Random): Kind {
  if (random() < 0.7) {
    return { kind: "object", fields: makeFields(random, between(random, 2, 4), 1) };
  }

  return { kind: pick(random, primitiveKinds) };
}

function makeTasks(): Task[] {
  const random = createRandom(generatorSeed);

  return Array.from({ length: taskCount }, (_, index) => {
    const params = makeFields(random, between(random, 3, 8), 0).map((field, position) => ({ ...field, name: `p${position}` }));
    const output = makeOutput(random);
    const codes = index % 2 === 0 ? [`E${index}_A`, `E${index}_B`] : [];
    const previous: PreviousTask = index % nodesPerWorkflow === 0 ? { kind: "none" } : { kind: "task", index: index - 1 };

    return { index, params, output, codes, previous };
  });
}

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

function valueOf(kind: Kind): string {
  switch (kind.kind) {
    case "string":
      return `"text"`;
    case "uuid":
      return `"0190a3c4-0000-7000-8000-000000000001"`;
    case "int":
      return "7";
    case "float":
      return "1.5";
    case "bool":
      return "true";
    case "decimal":
      return "dec";
    case "instant":
      return "at";
    case "date":
      return "day";
    case "enum":
      return `"${itemAt(kind.members, 0, "enum member")}"`;
    case "array":
      return `[${valueOf(kind.item)}]`;
    case "object":
      return `{ ${sentFields(kind.fields)} }`;
    default:
      return assertNever(kind);
  }
}

/** The fields that a caller must give, as `name: value` pairs. */
function sentFields(fields: readonly Field[]): string {
  return fields
    .filter((field) => field.presence !== "optional")
    .map((field) => `${field.name}: ${valueOf(field.type)}`)
    .join(", ");
}

const pad = (value: number): string => String(value).padStart(2, "0");
const fileOfTask = (index: number): number => Math.floor(index / tasksPerFile);

function chunks<T>(items: readonly T[], size: number): T[][] {
  return Array.from({ length: Math.ceil(items.length / size) }, (_, chunk) => items.slice(chunk * size, (chunk + 1) * size));
}

/** One import line per task file, in the order of first use. */
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
