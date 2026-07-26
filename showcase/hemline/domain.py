# showcase/hemline/domain.py
"""Hemline's domain vocabulary: error codes, entities, and task payloads.

Entities mirror rows in the `hemline_*` tables. Payload models are what tasks
return inside `TaskResult(ok=...)`; they are the typed contract the dashboard
renders and downstream nodes consume through `args_from`.
"""

from __future__ import annotations

from datetime import datetime
from typing import Final, Literal

from pydantic import BaseModel

# --- Domain error codes -------------------------------------------------
# Plain `str`, never `str, Enum`: user codes must survive a JSON round trip
# as strings, and none of them may collide with a reserved built-in code.

CARD_DECLINED: Final[str] = 'CARD_DECLINED'
PSP_UNAVAILABLE: Final[str] = 'PSP_UNAVAILABLE'
PAYMENT_ALREADY_CAPTURED: Final[str] = 'PAYMENT_ALREADY_CAPTURED'
INSUFFICIENT_STOCK: Final[str] = 'INSUFFICIENT_STOCK'
UNKNOWN_SKU: Final[str] = 'UNKNOWN_SKU'
ORDER_NOT_FOUND: Final[str] = 'ORDER_NOT_FOUND'
SHIPMENT_NOT_FOUND: Final[str] = 'SHIPMENT_NOT_FOUND'
RETURN_NOT_FOUND: Final[str] = 'RETURN_NOT_FOUND'
DAMAGED_ITEM: Final[str] = 'DAMAGED_ITEM'
NO_WORKFLOW_CONTEXT: Final[str] = 'NO_WORKFLOW_CONTEXT'
QUORUM_NOT_MET: Final[str] = 'QUORUM_NOT_MET'
CDN_REJECTED: Final[str] = 'CDN_REJECTED'
ORIGIN_REJECTED: Final[str] = 'ORIGIN_REJECTED'
SEARCH_INDEX_STALE: Final[str] = 'SEARCH_INDEX_STALE'
RECONCILIATION_MISMATCH: Final[str] = 'RECONCILIATION_MISMATCH'
COURIER_UNAVAILABLE: Final[str] = 'COURIER_UNAVAILABLE'
SUPPLIER_TIMEOUT: Final[str] = 'SUPPLIER_TIMEOUT'
DATA_CORRUPTION: Final[str] = 'DATA_CORRUPTION'
LOYALTY_ENGINE_BUG: Final[str] = 'LOYALTY_ENGINE_BUG'
STORE_UNAVAILABLE: Final[str] = 'STORE_UNAVAILABLE'

# Plain assignments, not PEP 695 `type` aliases: strict-serde inspects the
# annotations of every model that appears in a task's TaskResult ok slot, and a
# `TypeAliasType` is not on its allow list (HRS-105). An implicit alias *is*
# the Literal at runtime, so it resolves.
OrderStatus = Literal[
    'placed',
    'validated',
    'reserved',
    'authorized',
    'packed',
    'shipped',
    'captured',
    'failed',
]

PaymentKind = Literal['authorization', 'capture', 'refund']

ReturnStatus = Literal['received', 'inspected', 'restocked', 'written_off']

ItemCondition = Literal['resellable', 'damaged']


# --- Entities -----------------------------------------------------------


class Product(BaseModel):
    """A catalog item."""

    sku: str
    name: str
    category: str
    price_cents: int


class StockLevel(BaseModel):
    """On-hand and reserved units for one SKU."""

    sku: str
    on_hand: int
    reserved: int

    @property
    def available(self) -> int:
        return self.on_hand - self.reserved


class CatalogEntry(BaseModel):
    """A product joined to its stock level — what the order builder shops from."""

    product: Product
    stock: StockLevel


class OrderLine(BaseModel):
    """One line of an order. `size_code` drives the promotions size table."""

    line_no: int
    sku: str
    size_code: str
    quantity: int
    unit_price_cents: int

    @property
    def line_total_cents(self) -> int:
        return self.quantity * self.unit_price_cents


class Order(BaseModel):
    """A placed order with its lines. Written before fulfillment starts."""

    order_id: str
    customer_id: str
    status: OrderStatus
    total_cents: int
    lines: list[OrderLine]
    created_at: datetime


class PaymentIntent(BaseModel):
    """One row of the payment ledger: an authorization, capture, or refund."""

    payment_id: str
    order_id: str
    kind: PaymentKind
    amount_cents: int
    psp_reference: str
    created_at: datetime


class Shipment(BaseModel):
    """The shipment the child workflow books, labels, and seeds tracking for."""

    shipment_id: str
    order_id: str
    courier: str
    express: bool
    attempts: int
    booking_reference: str | None
    label_url: str | None
    tracking_code: str | None


# --- Task payloads ------------------------------------------------------


class OrderValidation(BaseModel):
    """What `validate_order` confirmed against the stored order."""

    order_id: str
    line_count: int
    total_cents: int


class StockReservation(BaseModel):
    """One reserved order line. `replayed` marks an idempotent re-run."""

    order_id: str
    sku: str
    quantity: int
    available_after: int
    replayed: bool


class StockRelease(BaseModel):
    """Units handed back to available stock."""

    sku: str
    quantity: int
    available_after: int


class WarehouseAllocation(BaseModel):
    """Which warehouse will pick the order."""

    order_id: str
    warehouse_code: str
    distance_km: int


class PaymentAuthorization(BaseModel):
    """A hold on the customer's card. Consumed by `capture_payment`."""

    order_id: str
    authorization_id: str
    amount_cents: int
    psp_reference: str
    attempt: int
    replayed: bool


class PaymentCapture(BaseModel):
    """Settled funds — the flagship workflow's output."""

    order_id: str
    capture_id: str
    authorization_id: str
    amount_cents: int
    replayed: bool


class PaymentRefund(BaseModel):
    """Money returned for an accepted return."""

    order_id: str
    refund_id: str
    amount_cents: int


class PickPack(BaseModel):
    """Warehouse work, stamped with the `WorkflowMeta` the engine injected."""

    order_id: str
    station: str
    units_picked: int
    workflow_id: str | None
    task_index: int | None


class Invoice(BaseModel):
    """A rendered invoice."""

    order_id: str
    invoice_number: str
    total_cents: int
    render_ms: int


class CourierBooking(BaseModel):
    """A carrier slot. `attempt` shows how many tries the flake cost."""

    order_id: str
    courier: str
    express: bool
    booking_reference: str
    attempt: int
    replayed: bool


class ShippingLabel(BaseModel):
    """A printed label."""

    order_id: str
    label_url: str
    label_format: str


class TrackingSeed(BaseModel):
    """The tracking code handed to the customer — the child workflow's output."""

    order_id: str
    courier: str
    tracking_code: str
    tracking_url: str


class EmailReceipt(BaseModel):
    """A sent email. `template` distinguishes the confirmation from the apology."""

    order_id: str
    template: str
    recipient: str


class PromotionOutcome(BaseModel):
    """Discounts applied to an order."""

    order_id: str
    discount_cents: int
    applied_codes: list[str]


class LoyaltyPoints(BaseModel):
    """Points earned by a customer for one order."""

    customer_id: str
    order_id: str
    points: int
    tier: str


class SupplierFeed(BaseModel):
    """One supplier feed pull."""

    supplier: str
    sku_count: int
    changed_count: int


class StockUpdate(BaseModel):
    """Result of applying a feed's stock deltas."""

    supplier: str
    applied: int
    skipped: int


class ReturnCase(BaseModel):
    """A customer return working its way through inspection."""

    return_id: str
    order_id: str
    sku: str
    quantity: int
    status: ReturnStatus
    condition: ItemCondition | None
    created_at: datetime


class ReturnReceipt(BaseModel):
    """A return booked into the returns desk."""

    return_id: str
    order_id: str
    sku: str
    quantity: int


class Inspection(BaseModel):
    """What the inspector found. A damaged item pauses the workflow."""

    return_id: str
    sku: str
    condition: ItemCondition
    notes: str


class RestockDecision(BaseModel):
    """Where the returned item went."""

    return_id: str
    sku: str
    quantity: int
    outcome: Literal['restocked', 'written_off']
    available_after: int | None


class RestockPlan(BaseModel):
    """What the quorum aggregate did with the supplier feeds it could read."""

    suppliers_reporting: list[str]
    suppliers_missing: list[str]
    skus_adjusted: int
    units_added: int


class SalesRollup(BaseModel):
    """Daily sales aggregate."""

    orders_counted: int
    gross_cents: int
    captured_cents: int


class AbandonedCartSweep(BaseModel):
    """Orders that never reached a capture.

    `orders_counted` is set only when the sweep ran inside `daily_report` and
    received the rollup's result.
    """

    swept: int
    oldest_order_id: str | None
    orders_counted: int | None = None


class RegionalRollup(BaseModel):
    """Sales aggregate for one region."""

    region: str
    orders_counted: int
    gross_cents: int


class RetentionAudit(BaseModel):
    """What a retention policy would prune. Reports only, never deletes."""

    older_than_days: int
    orders_examined: int
    rows_prunable: int


class CatalogChunk(BaseModel):
    """One chunk of a long catalog import."""

    chunk_index: int
    rows: int
    checksum: str


class PricePush(BaseModel):
    """A price published to one edge."""

    sku: str
    price_cents: int
    target: str


class CacheWarm(BaseModel):
    """A warmed cache edge."""

    target: str
    keys_warmed: int


class SearchPrewarm(BaseModel):
    """A prewarmed search index."""

    documents: int
    index_name: str


class PriceUpdate(BaseModel):
    """A flash-sale price change."""

    sku: str
    was_cents: int
    now_cents: int


class ReconciliationReport(BaseModel):
    """Authorizations checked against captures."""

    authorizations: int
    captures: int
    unmatched: int


class ExportManifest(BaseModel):
    """A data export. The chaos scenario kills this one mid-flight."""

    export_id: str
    rows: int


class ShippingNotice(BaseModel):
    """An SMS telling the customer their order shipped."""

    order_id: str
    recipient: str
    tracking_code: str


class MarketingBlast(BaseModel):
    """One segment of a marketing send."""

    segment: str
    recipients: int
