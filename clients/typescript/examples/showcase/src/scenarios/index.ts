// The scenarios of the showcase: the producer commands of the entry, next to the commands of `cli.run`.
//
//   <entry> seed | steady [--orders N] [--cover-errors] [--pace X] | rush | problem-child | bulk-import | flash-sale |
//           chaos | maintenance | simulate
//
// Exit codes: 0 when the scenario ran to its end; 1 when a step failed (its line is on stderr); 2 for arguments that
// do not parse.

import { parseArgs } from "node:util";

import type { Result } from "horsies";

import { err, ok } from "../result.ts";
import { STEADY_MAX_ORDERS, STEADY_MAX_PACE } from "../tuning.ts";
import { runBulkImport } from "./bulk-import.ts";
import { runChaos } from "./chaos.ts";
import { runFlashSale } from "./flash-sale.ts";
import { runMaintenance } from "./maintenance.ts";
import { runProblemChild } from "./problem-child.ts";
import { runRush } from "./rush.ts";
import { runSeed } from "./seed.ts";
import { runSimulate } from "./simulate.ts";
import { type OrderCount, runSteady, type SteadyOptions } from "./steady.ts";

export { runSteady, type SteadyOptions, type SteadyReport } from "./steady.ts";

export type ScenarioName =
  | "seed"
  | "steady"
  | "rush"
  | "problem-child"
  | "bulk-import"
  | "flash-sale"
  | "chaos"
  | "maintenance"
  | "simulate";

/** Each scenario name, by itself: the record names each member of `ScenarioName` once. */
const SCENARIO_NAME_SET: { readonly [Name in ScenarioName]: Name } = {
  seed: "seed",
  steady: "steady",
  rush: "rush",
  "problem-child": "problem-child",
  "bulk-import": "bulk-import",
  "flash-sale": "flash-sale",
  chaos: "chaos",
  maintenance: "maintenance",
  simulate: "simulate",
};

export const SCENARIO_NAMES: readonly ScenarioName[] = Object.values(SCENARIO_NAME_SET);

/** The exit codes of a scenario. */
export const SCENARIO_DONE = 0;
export const SCENARIO_FAILED = 1;
export const SCENARIO_USAGE = 2;

/** The usage line of `steady`. */
const STEADY_USAGE = "<entry> steady [--orders N] [--cover-errors] [--pace X]";

/** The pace of a run that names none: the tuned inter-arrival times. */
const DEFAULT_PACE = 1;

/** The scenario that `text` names, or none. Pure. */
export function scenarioNameOf(text: string): { readonly kind: "scenario"; readonly name: ScenarioName } | { readonly kind: "notScenario" } {
  const name = SCENARIO_NAMES.find((known) => known === text);

  return name === undefined ? { kind: "notScenario" } : { kind: "scenario", name };
}

/** The error codes of `parseArgs` for arguments that do not parse. */
const PARSE_ARGS_ERROR_CODES: readonly string[] = [
  "ERR_PARSE_ARGS_UNKNOWN_OPTION",
  "ERR_PARSE_ARGS_INVALID_OPTION_VALUE",
  "ERR_PARSE_ARGS_UNEXPECTED_POSITIONAL",
];

/** The flags of `steady`, as `parseArgs` reads them. */
type SteadyFlags = { readonly orders?: string; readonly "cover-errors"?: boolean; readonly pace?: string };

function steadyFlags(args: readonly string[]): Result<SteadyFlags, string> {
  try {
    const parsed = parseArgs({
      args: [...args],
      options: { orders: { type: "string" }, "cover-errors": { type: "boolean" }, pace: { type: "string" } },
      strict: true,
      allowPositionals: false,
    });

    return ok(parsed.values);
  } catch (cause) {
    if (cause instanceof TypeError && "code" in cause && PARSE_ARGS_ERROR_CODES.some((code) => code === cause.code)) {
      return err(cause.message);
    }

    throw cause;
  }
}

/** The order count of `--orders`: a whole number from 1 to `STEADY_MAX_ORDERS`, or open when left out. Pure. */
function orderCountOf(text: string | undefined): Result<OrderCount, string> {
  if (text === undefined) {
    return ok({ kind: "untilStopped" });
  }

  const orders = Number(text);

  return Number.isSafeInteger(orders) && orders >= 1 && orders <= STEADY_MAX_ORDERS
    ? ok({ kind: "bounded", orders })
    : err(`--orders must be a whole number from 1 to ${String(STEADY_MAX_ORDERS)}, not ${JSON.stringify(text)}`);
}

/** The pace of `--pace`: a number from 1 to `STEADY_MAX_PACE`, or 1 when left out. Pure. */
function paceOf(text: string | undefined): Result<number, string> {
  if (text === undefined) {
    return ok(DEFAULT_PACE);
  }

  const pace = Number(text);

  return Number.isFinite(pace) && pace >= 1 && pace <= STEADY_MAX_PACE
    ? ok(pace)
    : err(`--pace must be a number from 1 to ${String(STEADY_MAX_PACE)}, not ${JSON.stringify(text)}`);
}

/** The options of `steady` of `args`, or the usage error. Pure. */
export function parseSteadyArgs(args: readonly string[]): Result<SteadyOptions, string> {
  const flags = steadyFlags(args);

  if (!flags.ok) {
    return flags;
  }

  const orders = orderCountOf(flags.value.orders);
  const pace = paceOf(flags.value.pace);

  if (!orders.ok) {
    return orders;
  }

  if (!pace.ok) {
    return pace;
  }

  return ok({ orders: orders.value, errors: flags.value["cover-errors"] === true ? "coverErrors" : "noCover", pace: pace.value });
}

/** The run of a scenario that takes no arguments. */
function runOf(name: Exclude<ScenarioName, "steady">): () => Promise<Result<void, string>> {
  switch (name) {
    case "seed":
      return runSeed;
    case "rush":
      return runRush;
    case "problem-child":
      return runProblemChild;
    case "bulk-import":
      return runBulkImport;
    case "flash-sale":
      return runFlashSale;
    case "chaos":
      return runChaos;
    case "maintenance":
      return runMaintenance;
    case "simulate":
      return runSimulate;
    default: {
      const unexpected: never = name;

      throw new TypeError(`runOf: a scenario outside its type: ${String(unexpected)}`);
    }
  }
}

function usageExit(name: ScenarioName, usage: string, message: string): number {
  process.stderr.write(`usage: ${usage}\nacme ${name}: error: ${message}\n`);

  return SCENARIO_USAGE;
}

function endExit(name: ScenarioName, ran: Result<unknown, string>): number {
  if (ran.ok) {
    return SCENARIO_DONE;
  }

  process.stderr.write(`acme ${name}: ${ran.error}\n`);

  return SCENARIO_FAILED;
}

/** Run the scenario `name` with the arguments after its name: the exit code. */
export async function runScenario(name: ScenarioName, args: readonly string[]): Promise<number> {
  switch (name) {
    case "steady": {
      const options = parseSteadyArgs(args);

      return options.ok ? endExit(name, await runSteady(options.value)) : usageExit(name, STEADY_USAGE, options.error);
    }

    case "seed":
    case "rush":
    case "problem-child":
    case "bulk-import":
    case "flash-sale":
    case "chaos":
    case "maintenance":
    case "simulate":
      return args.length === 0 ? endExit(name, await runOf(name)()) : usageExit(name, `<entry> ${name}`, "takes no arguments");
    default: {
      const unexpected: never = name;

      throw new TypeError(`runScenario: a scenario outside its type: ${String(unexpected)}`);
    }
  }
}
