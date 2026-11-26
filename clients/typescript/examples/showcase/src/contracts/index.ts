// Every task contract of the app, in one list: the `tasks` of `Horsies.create`.

import { ANALYTICS_TASKS } from "./analytics.ts";
import { INVENTORY_TASKS } from "./inventory.ts";
import { NOTIFY_TASKS } from "./notify.ts";
import { ORDER_TASKS } from "./orders.ts";
import { PAYMENT_TASKS } from "./payments.ts";
import { PROMOTION_TASKS } from "./promotions.ts";
import { RETURN_TASKS } from "./returns.ts";
import { SHIPPING_TASKS } from "./shipping.ts";

export const ALL_TASKS = [
  ...ANALYTICS_TASKS,
  ...INVENTORY_TASKS,
  ...NOTIFY_TASKS,
  ...ORDER_TASKS,
  ...PAYMENT_TASKS,
  ...PROMOTION_TASKS,
  ...RETURN_TASKS,
  ...SHIPPING_TASKS,
] as const;
