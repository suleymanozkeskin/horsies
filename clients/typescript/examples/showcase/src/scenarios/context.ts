// What every scenario uses: the database of the settings, the store, the app, and the sends that several scenarios
// make. A step that fails gives the text of its failure: the scenario writes it to stderr and ends.

import type { Result } from "horsies";

import { appRefusedText, buildShowcase, type ShowcaseApp } from "../app.ts";
import { applyPromotions, computeLoyaltyPoints } from "../contracts/promotions.ts";
import type { Order } from "../domain.ts";
import { err, ok } from "../result.ts";
import { type DatabaseSettings, processDatabaseSettings, settingsRefusedText } from "../settings.ts";
import { closeStore, openStore, type Store, storeFailureText } from "../store/connection.ts";
import { ensureDatabase, ensureSchema } from "../store/schema.ts";
import { orderFulfillment } from "../workflows/order-fulfillment.ts";

/** The database, the store and the app of one scenario run. The scenario closes the store at its end. */
export type ScenarioContext = {
  readonly settings: DatabaseSettings;
  readonly store: Store;
  readonly showcase: ShowcaseApp;
};

/** `CODE: message` of a failed send or start. Pure. */
export function sendErrorText(error: { readonly code: string; readonly message: string }): string {
  return `${error.code}: ${error.message}`;
}

/**
 * The settings of the process, the Acme database (created when the server has none), the store with the Acme tables,
 * and the app. Effects: reads the environment; connects; creates the database and the tables that do not exist.
 */
export async function prepareScenario(): Promise<Result<ScenarioContext, string>> {
  const settings = processDatabaseSettings();

  if (!settings.ok) {
    return err(settingsRefusedText(settings.error));
  }

  const created = await ensureDatabase(settings.value);

  if (!created.ok) {
    return err(storeFailureText(created.error));
  }

  const store = openStore(settings.value.url);
  const schema = await ensureSchema(store);

  if (!schema.ok) {
    await closeStore(store);

    return err(storeFailureText(schema.error));
  }

  const showcase = buildShowcase(settings.value);

  if (!showcase.ok) {
    await closeStore(store);

    return err(appRefusedText(showcase.error));
  }

  return ok({ settings: settings.value, store, showcase: showcase.value });
}

/** Start the order workflow of `order`: its workflow id. */
export async function startOrder(context: ScenarioContext, order: Order): Promise<Result<string, string>> {
  const started = await context.showcase.app.start(orderFulfillment, { order });

  return started.ok ? ok(started.value.workflowId) : err(`start order_fulfillment ${order.orderId}: ${sendErrorText(started.error)}`);
}

/** Send the promotion and the loyalty tasks of `order`, outside its workflow. */
export async function sendStandalone(context: ScenarioContext, order: Order): Promise<Result<void, string>> {
  const app = context.showcase.app;
  const promotions = await app.send(applyPromotions, { orderId: order.orderId });

  if (!promotions.ok) {
    return err(`send apply_promotions ${order.orderId}: ${sendErrorText(promotions.error)}`);
  }

  const loyalty = await app.send(computeLoyaltyPoints, { customerId: order.customerId, orderId: order.orderId });

  return loyalty.ok ? ok(undefined) : err(`send compute_loyalty_points ${order.orderId}: ${sendErrorText(loyalty.error)}`);
}

/** The link of a workflow run on the monitoring page. Pure. */
export function workflowLink(baseUrl: string, workflowId: string): string {
  return `${baseUrl}/workflows?run=${workflowId}`;
}

/** Prepare a scenario, run `body` with it, and close the store at the end, also after a failure of `body`. */
export async function withScenario<T>(body: (context: ScenarioContext) => Promise<Result<T, string>>): Promise<Result<T, string>> {
  const context = await prepareScenario();

  if (!context.ok) {
    return context;
  }

  try {
    return await body(context.value);
  } finally {
    await closeStore(context.value.store);
  }
}
