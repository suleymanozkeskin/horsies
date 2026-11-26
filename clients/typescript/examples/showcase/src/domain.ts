// The Acme domain: the wire types of the task outputs and of the order (the params of the order workflow), the order
// statuses, and the error codes of the Acme tasks.
//
// Each wire type is a value (`h.object`) and a type of the same name (`h.Infer`). The worker stores each output in the
// form that its wire type gives, and every reader decodes it with the same type.

import { h } from "horsies";

/** The status of an order, in the order that the order workflow moves it. */
export const OrderStatus = h.enum(["placed", "validated", "reserved", "authorized", "packed", "shipped", "captured", "failed"]);

export type OrderStatus = h.Infer<typeof OrderStatus>;

/** The statuses of an order that takes a stock reservation. */
export const RESERVABLE_STATUSES: readonly OrderStatus[] = ["placed", "validated"];

/** The statuses of an order that a failure moves to `failed`. */
export const ACTIVE_STATUSES: readonly OrderStatus[] = ["placed", "validated", "reserved", "authorized"];

/** The status of a return case. */
export const ReturnStatus = h.enum(["received", "inspected", "restocked", "written_off"]);

export type ReturnStatus = h.Infer<typeof ReturnStatus>;

/** The condition of a returned item. */
export const ItemCondition = h.enum(["resellable", "damaged"]);

export type ItemCondition = h.Infer<typeof ItemCondition>;

/** The kind of a payment row. */
export type PaymentKind = "authorization" | "capture";

export const OrderLine = h.object({
  lineNo: h.int(),
  sku: h.string(),
  sizeCode: h.string(),
  quantity: h.int(),
  unitPriceCents: h.int(),
});

export type OrderLine = h.Infer<typeof OrderLine>;

/** An order: the params of the order workflow and a row of the store. */
export const Order = h.object({
  orderId: h.string(),
  customerId: h.string(),
  status: OrderStatus,
  totalCents: h.int(),
  lines: h.array(OrderLine),
  createdAt: h.instant(),
});

export type Order = h.Infer<typeof Order>;

export const OrderValidation = h.object({ orderId: h.string(), lineCount: h.int(), totalCents: h.int() });

export type OrderValidation = h.Infer<typeof OrderValidation>;

export const StockReservation = h.object({
  orderId: h.string(),
  sku: h.string(),
  quantity: h.int(),
  availableAfter: h.int(),
  replayed: h.boolean(),
});

export type StockReservation = h.Infer<typeof StockReservation>;

export const StockRelease = h.object({ sku: h.string(), quantity: h.int(), availableAfter: h.int() });

export type StockRelease = h.Infer<typeof StockRelease>;

export const StocktakeSummary = h.object({ skusToppedUp: h.int(), reservationsCleared: h.int(), targetUnits: h.int() });

export type StocktakeSummary = h.Infer<typeof StocktakeSummary>;

export const WarehouseAllocation = h.object({ orderId: h.string(), warehouseCode: h.string(), distanceKm: h.int() });

export type WarehouseAllocation = h.Infer<typeof WarehouseAllocation>;

export const PaymentAuthorization = h.object({
  orderId: h.string(),
  authorizationId: h.string(),
  amountCents: h.int(),
  pspReference: h.string(),
  attempt: h.int(),
  replayed: h.boolean(),
});

export type PaymentAuthorization = h.Infer<typeof PaymentAuthorization>;

export const PaymentCapture = h.object({
  orderId: h.string(),
  captureId: h.string(),
  authorizationId: h.string(),
  amountCents: h.int(),
  replayed: h.boolean(),
});

export type PaymentCapture = h.Infer<typeof PaymentCapture>;

export const PaymentRefund = h.object({
  orderId: h.string(),
  refundId: h.string(),
  amountCents: h.int(),
  disputedAt: h.instant(),
});

export type PaymentRefund = h.Infer<typeof PaymentRefund>;

export const PickPack = h.object({ orderId: h.string(), station: h.string(), unitsPicked: h.int() });

export type PickPack = h.Infer<typeof PickPack>;

export const Invoice = h.object({ orderId: h.string(), invoiceNumber: h.string(), totalCents: h.int() });

export type Invoice = h.Infer<typeof Invoice>;

export const CourierBooking = h.object({
  orderId: h.string(),
  courier: h.string(),
  express: h.boolean(),
  bookingReference: h.string(),
  attempt: h.int(),
  replayed: h.boolean(),
});

export type CourierBooking = h.Infer<typeof CourierBooking>;

export const ShippingLabel = h.object({ orderId: h.string(), labelUrl: h.string(), labelFormat: h.enum(["A5", "A6"]) });

export type ShippingLabel = h.Infer<typeof ShippingLabel>;

export const TrackingSeed = h.object({
  orderId: h.string(),
  courier: h.string(),
  trackingCode: h.string(),
  trackingUrl: h.string(),
});

export type TrackingSeed = h.Infer<typeof TrackingSeed>;

export const EmailReceipt = h.object({ orderId: h.string(), template: h.string(), recipient: h.string() });

export type EmailReceipt = h.Infer<typeof EmailReceipt>;

export const PromotionOutcome = h.object({
  orderId: h.string(),
  discountCents: h.int(),
  appliedCodes: h.array(h.string()),
});

export type PromotionOutcome = h.Infer<typeof PromotionOutcome>;

export const LoyaltyPoints = h.object({ customerId: h.string(), orderId: h.string(), points: h.int(), tier: h.string() });

export type LoyaltyPoints = h.Infer<typeof LoyaltyPoints>;

export const SupplierFeed = h.object({ supplier: h.string(), skuCount: h.int(), changedCount: h.int() });

export type SupplierFeed = h.Infer<typeof SupplierFeed>;

export const StockUpdate = h.object({ suppliersApplied: h.array(h.string()), applied: h.int(), skipped: h.int() });

export type StockUpdate = h.Infer<typeof StockUpdate>;

/** A return case: a row of the store. */
export const ReturnCase = h.object({
  returnId: h.string(),
  orderId: h.string(),
  sku: h.string(),
  quantity: h.int(),
  status: ReturnStatus,
  condition: ItemCondition.nullable(),
  createdAt: h.instant(),
});

export type ReturnCase = h.Infer<typeof ReturnCase>;

export const ReturnReceipt = h.object({ returnId: h.string(), orderId: h.string(), sku: h.string(), quantity: h.int() });

export type ReturnReceipt = h.Infer<typeof ReturnReceipt>;

export const Inspection = h.object({ returnId: h.string(), sku: h.string(), condition: ItemCondition, notes: h.string() });

export type Inspection = h.Infer<typeof Inspection>;

/** What a return ended as: back on the shelf (with the units now available), or written off. */
export const RestockOutcome = h.union("kind", [
  h.object({ kind: h.literal("restocked"), availableAfter: h.int() }),
  h.object({ kind: h.literal("writtenOff") }),
]);

export type RestockOutcome = h.Infer<typeof RestockOutcome>;

export const RestockDecision = h.object({
  returnId: h.string(),
  sku: h.string(),
  quantity: h.int(),
  outcome: RestockOutcome,
});

export type RestockDecision = h.Infer<typeof RestockDecision>;

export const SalesRollup = h.object({ ordersCounted: h.int(), grossCents: h.int(), capturedCents: h.int() });

export type SalesRollup = h.Infer<typeof SalesRollup>;

/** The oldest stranded order of a sweep, or none. */
export const OldestOrder = h.union("kind", [
  h.object({ kind: h.literal("found"), orderId: h.string() }),
  h.object({ kind: h.literal("none") }),
]);

export type OldestOrder = h.Infer<typeof OldestOrder>;

export const AbandonedCartSweep = h.object({ swept: h.int(), oldest: OldestOrder });

export type AbandonedCartSweep = h.Infer<typeof AbandonedCartSweep>;

export const RegionalRollup = h.object({ region: h.string(), ordersCounted: h.int(), grossCents: h.int() });

export type RegionalRollup = h.Infer<typeof RegionalRollup>;

export const RetentionAudit = h.object({ olderThanDays: h.int(), ordersExamined: h.int(), rowsPrunable: h.int() });

export type RetentionAudit = h.Infer<typeof RetentionAudit>;

export const CatalogChunk = h.object({ chunkIndex: h.int(), rows: h.int(), checksum: h.string() });

export type CatalogChunk = h.Infer<typeof CatalogChunk>;

export const PricePush = h.object({ sku: h.string(), priceCents: h.int(), target: h.string() });

export type PricePush = h.Infer<typeof PricePush>;

export const CacheWarm = h.object({ target: h.string(), keysWarmed: h.int() });

export type CacheWarm = h.Infer<typeof CacheWarm>;

export const SearchPrewarm = h.object({ documents: h.int(), indexName: h.string() });

export type SearchPrewarm = h.Infer<typeof SearchPrewarm>;

export const PriceUpdate = h.object({ sku: h.string(), wasCents: h.int(), nowCents: h.int() });

export type PriceUpdate = h.Infer<typeof PriceUpdate>;

export const ReconciliationReport = h.object({ authorizations: h.int(), captures: h.int(), unmatched: h.int() });

export type ReconciliationReport = h.Infer<typeof ReconciliationReport>;

export const ExportManifest = h.object({ exportId: h.string(), rows: h.int() });

export type ExportManifest = h.Infer<typeof ExportManifest>;

export const ShippingNotice = h.object({ orderId: h.string(), recipient: h.string(), trackingCode: h.string() });

export type ShippingNotice = h.Infer<typeof ShippingNotice>;

export const MarketingBlast = h.object({ segment: h.string(), recipients: h.int() });

export type MarketingBlast = h.Infer<typeof MarketingBlast>;
