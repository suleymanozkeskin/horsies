// `steady`: place orders at the tuned demand. Each order starts its order workflow and sends its promotion and loyalty
// tasks; every `RETURN_SPAWN_EVERY`-th order starts a return of its first line, and every `RESTOCK_SPAWN_EVERY`-th
// order starts a supplier restock.
//
// A bounded run (`--orders N`) places its orders with no pause. An open run waits the drawn inter-arrival time divided
// by the demand factor and by `--pace` between two orders; it stops at SIGINT or SIGTERM (the orders that run go on in
// the worker), and at `STEADY_MAX_ORDERS` orders.

import { setTimeout as sleep } from "node:timers/promises";

import type { Result } from "horsies";

import type { Order } from "../domain.ts";
import { err, ok } from "../result.ts";
import { demandFactor, integer } from "../simulate.ts";
import type { CatalogEntry } from "../store/catalog.ts";
import {
  RESTOCK_SPAWN_EVERY,
  RETURN_SPAWN_EVERY,
  STEADY_MAX_INTERARRIVAL_SECONDS,
  STEADY_MAX_ORDERS,
  STEADY_MIN_INTERARRIVAL_SECONDS,
} from "../tuning.ts";
import { restock } from "../workflows/restock.ts";
import { nextOrder, readCatalog } from "./catalog.ts";
import { type ScenarioContext, sendErrorText, sendStandalone, startOrder, withScenario, workflowLink } from "./context.ts";
import { coverFailureTable } from "./cover-errors.ts";
import { bullet, heading, say, webBaseUrl } from "./output.ts";
import { startReturn } from "./returns.ts";

/** How many orders a run places: `orders`, or orders until a stop signal (at most `STEADY_MAX_ORDERS`). */
export type OrderCount = { readonly kind: "bounded"; readonly orders: number } | { readonly kind: "untilStopped" };

/** Whether the run first sends one run of each row of the failure table. */
export type ErrorCover = "coverErrors" | "noCover";

export type SteadyOptions = {
  readonly orders: OrderCount;
  readonly errors: ErrorCover;
  /** The divisor of the pause between two orders of an open run: 1 to `STEADY_MAX_PACE`. */
  readonly pace: number;
};

/** Why the run stopped placing orders. */
export type SteadyEnd = "allPlaced" | "stopped" | "orderBound";

export type SteadyReport = {
  readonly ordersPlaced: number;
  readonly returnsStarted: number;
  readonly restocksStarted: number;
  readonly end: SteadyEnd;
};

/** The counts of a run so far. */
type Counts = { ordersPlaced: number; returnsStarted: number; restocksStarted: number };

const MILLISECONDS_PER_SECOND = 1_000;

/** Place the next order: write it, start its workflow, send its standalone tasks. */
async function placeAndStart(context: ScenarioContext, catalog: readonly CatalogEntry[]): Promise<Result<Order, string>> {
  const order = await nextOrder(context.store, catalog);

  if (!order.ok) {
    return order;
  }

  say(`${order.value.orderId}  ${String(order.value.lines.length)} line(s), ${(order.value.totalCents / 100).toFixed(2)} EUR`);

  const started = await startOrder(context, order.value);

  if (!started.ok) {
    return started;
  }

  say(`  workflow: ${workflowLink(webBaseUrl(), started.value)}`);

  const standalone = await sendStandalone(context, order.value);

  return standalone.ok ? order : standalone;
}

/** Start the return of the first line of `order`. */
async function spawnReturn(context: ScenarioContext, order: Order): Promise<Result<void, string>> {
  const [line] = order.lines;

  if (line === undefined) {
    return err(`order ${order.orderId} has no returnable line`);
  }

  const started = await startReturn(context, order.orderId, line.sku, line.quantity);

  if (!started.ok) {
    return started;
  }

  say(`  return ${started.value.returnId}  ->  ${workflowLink(webBaseUrl(), started.value.workflowId)}`);

  return ok(undefined);
}

/** Start a supplier restock. */
async function spawnRestock(context: ScenarioContext): Promise<Result<void, string>> {
  const started = await context.showcase.app.start(restock, {});

  if (!started.ok) {
    return err(`start restock: ${sendErrorText(started.error)}`);
  }

  say(`  restock  ->  ${workflowLink(webBaseUrl(), started.value.workflowId)}`);

  return ok(undefined);
}

/** Place one order and the return and the restock that its ordinal calls for. */
async function placeOne(context: ScenarioContext, catalog: readonly CatalogEntry[], counts: Counts): Promise<Result<Order, string>> {
  const order = await placeAndStart(context, catalog);

  if (!order.ok) {
    return order;
  }

  counts.ordersPlaced += 1;

  if (counts.ordersPlaced % RETURN_SPAWN_EVERY === 0) {
    const spawned = await spawnReturn(context, order.value);

    if (!spawned.ok) {
      return spawned;
    }

    counts.returnsStarted += 1;
  }

  if (counts.ordersPlaced % RESTOCK_SPAWN_EVERY === 0) {
    const spawned = await spawnRestock(context);

    if (!spawned.ok) {
      return spawned;
    }

    counts.restocksStarted += 1;
  }

  return order;
}

/** The pause after `order` in an open run, in milliseconds. Effects: reads the clock. */
function interarrivalMs(order: Order, pace: number): number {
  const seconds = integer(STEADY_MIN_INTERARRIVAL_SECONDS, STEADY_MAX_INTERARRIVAL_SECONDS, [order.orderId, "interarrival"]);

  return (seconds * MILLISECONDS_PER_SECOND) / demandFactor(Temporal.Now.instant()) / pace;
}

/** Whether a pause ended at its time or at the stop signal. */
type PauseEnd = "elapsed" | "stopped";

async function pause(milliseconds: number, signal: AbortSignal): Promise<PauseEnd> {
  try {
    await sleep(milliseconds, undefined, { signal });

    return "elapsed";
  } catch (cause) {
    if (signal.aborted) {
      return "stopped";
    }

    throw cause;
  }
}

/** The rules of an order count: the most orders, whether the run pauses between orders, and the end at the limit. */
type CountRules = { readonly limit: number; readonly between: "pause" | "noPause"; readonly endAtLimit: SteadyEnd };

/** The rules of `count`. Pure. */
function countRules(count: OrderCount): CountRules {
  switch (count.kind) {
    case "bounded":
      return { limit: count.orders, between: "noPause", endAtLimit: "allPlaced" };
    case "untilStopped":
      return { limit: STEADY_MAX_ORDERS, between: "pause", endAtLimit: "orderBound" };
    default: {
      const unexpected: never = count;

      throw new TypeError(`countRules: an order count outside its type: ${String(unexpected)}`);
    }
  }
}

/** Whether to stop after the pause that follows `order`. Effects: the pause of an open run. */
async function pauseAfter(order: Order, rules: CountRules, pace: number, signal: AbortSignal): Promise<PauseEnd> {
  switch (rules.between) {
    case "noPause":
      return "elapsed";
    case "pause":
      return pause(interarrivalMs(order, pace), signal);
    default: {
      const unexpected: never = rules.between;

      throw new TypeError(`pauseAfter: a pause rule outside its type: ${String(unexpected)}`);
    }
  }
}

/** Place the orders of `options` until the limit or the stop signal. */
async function placeOrders(
  context: ScenarioContext,
  catalog: readonly CatalogEntry[],
  options: SteadyOptions,
  signal: AbortSignal,
): Promise<Result<SteadyReport, string>> {
  const counts: Counts = { ordersPlaced: 0, returnsStarted: 0, restocksStarted: 0 };
  const rules = countRules(options.orders);

  while (counts.ordersPlaced < rules.limit) {
    const order = await placeOne(context, catalog, counts);

    if (!order.ok) {
      return order;
    }

    if (counts.ordersPlaced < rules.limit && (await pauseAfter(order.value, rules, options.pace, signal)) === "stopped") {
      return ok({ ...counts, end: "stopped" });
    }
  }

  return ok({ ...counts, end: rules.endAtLimit });
}

function whatToWatch(): void {
  const base = webBaseUrl();

  heading("what to watch");
  bullet(`${base}/workflows              every run, live`);
  bullet(`${base}/?retried=true          authorizations that outlived a provider outage`);
  bullet(`${base}/?error_code=CARD_DECLINED       declines: run one again, it declines again`);
  bullet(`${base}/?error_code=INSUFFICIENT_STOCK  the skipped nodes in the graph view`);
  bullet(`${base}/?error_code=UNHANDLED_EXCEPTION  the bundle pricing crash, as data`);
  bullet(`${base}/?error_code=DATA_CORRUPTION      the size code fault`);
  bullet(`${base}/?error_code=LOYALTY_ENGINE_BUG   the loyalty engine fault`);
  bullet(`${base}/workers                CPU and memory`);
  say("");
  bullet("pause a RUNNING workflow from its run page, then resume it");
  bullet("Ctrl-C stops placing orders; the orders that run already finish");
}

function countText(count: OrderCount): string {
  switch (count.kind) {
    case "bounded":
      return `${String(count.orders)} bounded orders`;
    case "untilStopped":
      return `orders until Ctrl-C (at most ${String(STEADY_MAX_ORDERS)})`;
    default: {
      const unexpected: never = count;

      throw new TypeError(`countText: an order count outside its type: ${String(unexpected)}`);
    }
  }
}

/** The end line of a run. Pure. */
function endText(report: SteadyReport): string {
  switch (report.end) {
    case "allPlaced":
    case "orderBound":
      return `placed ${String(report.ordersPlaced)} orders`;
    case "stopped":
      return `\nstopped after placing ${String(report.ordersPlaced)} orders`;
    default: {
      const unexpected: never = report.end;

      throw new TypeError(`endText: an end outside its type: ${String(unexpected)}`);
    }
  }
}

/** The stop signal of a run, and the release of its listeners. */
type StopSignal = { readonly signal: AbortSignal; readonly release: () => void };

/**
 * The stop signal of `count`: SIGINT or SIGTERM for an open run, whose listeners `release` removes; none for a bounded
 * run, which keeps the default signal action. Effects: adds the two listeners of an open run.
 */
function stopSignalFor(count: OrderCount): StopSignal {
  switch (count.kind) {
    case "bounded":
      return { signal: new AbortController().signal, release: () => undefined };
    case "untilStopped": {
      const controller = new AbortController();

      const stop = (): void => {
        controller.abort();
      };

      process.once("SIGINT", stop);
      process.once("SIGTERM", stop);

      return {
        signal: controller.signal,
        release: () => {
          process.off("SIGINT", stop);
          process.off("SIGTERM", stop);
        },
      };
    }

    default: {
      const unexpected: never = count;

      throw new TypeError(`stopSignalFor: an order count outside its type: ${String(unexpected)}`);
    }
  }
}

/** Send one run of each row of the failure table, when `errors` asks for it. */
async function coverIfAsked(context: ScenarioContext, catalog: readonly CatalogEntry[], errors: ErrorCover): Promise<Result<void, string>> {
  switch (errors) {
    case "coverErrors":
      return coverFailureTable(context, catalog);
    case "noCover":
      return ok(undefined);
    default: {
      const unexpected: never = errors;

      throw new TypeError(`coverIfAsked: an error cover outside its type: ${String(unexpected)}`);
    }
  }
}

/** Run `steady` with `options`: the counts of what it placed and started. */
export async function runSteady(options: SteadyOptions): Promise<Result<SteadyReport, string>> {
  heading("Acme Clothing — steady");

  return withScenario(async (context) => {
    const catalog = await readCatalog(context.store);

    if (!catalog.ok) {
      return catalog;
    }

    say(`database: ${context.settings.databaseName} (resolved from ${context.settings.source})`);
    bullet(`live dashboard: ${webBaseUrl()}`);
    bullet(`placing ${countText(options.orders)}`);
    whatToWatch();

    const covered = await coverIfAsked(context, catalog.value, options.errors);

    if (!covered.ok) {
      return covered;
    }

    const stop = stopSignalFor(options.orders);
    const report = await placeOrders(context, catalog.value, options, stop.signal).finally(stop.release);

    if (report.ok) {
      say(endText(report.value));
    }

    return report;
  });
}
