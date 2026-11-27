// The task model of the typecheck-speed projects: 500 generated tasks from a fixed seed. The control project
// (`control-project.ts`) and the app project (`app-project.ts`) both read this model, so the two projects hold
// the same tasks: the same parameters, outputs, codes, chains, workflows and schedules.

export const generatorSeed = 20261125;
export const taskCount = 500;
export const tasksPerFile = 10;
export const nodesPerWorkflow = 10;
export const workflowsPerFile = 10;
export const workflowFileCount = 5;
export const scheduleCount = 20;
export const scheduleTaskStep = 7;

/** A field type nests objects and arrays to at most this depth below a task parameter. */
const maxNestingDepth = 2;

const primitiveKinds = ["string", "int", "float", "bool", "uuid", "decimal", "instant", "date"] as const;
const enumMembers = ["red", "green", "blue"] as const;

export type PrimitiveKind = (typeof primitiveKinds)[number];

/** The type of one generated field. Its depth is at most `maxNestingDepth + 1`. */
export type Kind =
  | { readonly kind: PrimitiveKind }
  | { readonly kind: "enum"; readonly members: readonly string[] }
  | { readonly kind: "array"; readonly item: Kind }
  | { readonly kind: "object"; readonly fields: readonly Field[] };

export interface Field {
  readonly name: string;
  readonly type: Kind;
  readonly presence: "required" | "optional" | "nullable";
}

/** The task before this one in its workflow chain: the task takes that task's outcome as `prev`. */
export type PreviousTask = { readonly kind: "none" } | { readonly kind: "task"; readonly index: number };

export interface Task {
  readonly index: number;
  readonly params: readonly Field[];
  readonly output: Kind;
  readonly codes: readonly string[];
  readonly previous: PreviousTask;
  /** How a worker runs the task. The app project declares it; the control project does not. */
  readonly execution: ExecutionClass;
  /** The task retries on its first code. Only a task with codes retries. */
  readonly retries: boolean;
}

export const executionClasses = ["ISOLATED", "SHARED_ASYNC", "SHARED_THREAD"] as const;

export type ExecutionClass = (typeof executionClasses)[number];

/** Every fourth task retries: all of them have codes, because a code is an even index. */
const retryStep = 4;

/** A fraction in [0, 1) from the generator's sequence. */
type Random = () => number;

export function assertNever(value: never): never {
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

export function itemAt<T>(items: readonly T[], index: number, subject: string): T {
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

export function makeTasks(): Task[] {
  const random = createRandom(generatorSeed);

  return Array.from({ length: taskCount }, (_, index) => {
    const params = makeFields(random, between(random, 3, 8), 0).map((field, position) => ({ ...field, name: `p${position}` }));
    const output = makeOutput(random);
    const codes = index % 2 === 0 ? [`E${index}_A`, `E${index}_B`] : [];
    const previous: PreviousTask = index % nodesPerWorkflow === 0 ? { kind: "none" } : { kind: "task", index: index - 1 };

    const execution = itemAt(executionClasses, index % executionClasses.length, "execution class");

    return { index, params, output, codes, previous, execution, retries: index % retryStep === 0 };
  });
}

export function valueOf(kind: Kind): string {
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
export function sentFields(fields: readonly Field[]): string {
  return fields
    .filter((field) => field.presence !== "optional")
    .map((field) => `${field.name}: ${valueOf(field.type)}`)
    .join(", ");
}

export const pad = (value: number): string => String(value).padStart(2, "0");
export const fileOfTask = (index: number): number => Math.floor(index / tasksPerFile);

export function chunks<T>(items: readonly T[], size: number): T[][] {
  return Array.from({ length: Math.ceil(items.length / size) }, (_, chunk) => items.slice(chunk * size, (chunk + 1) * size));
}

