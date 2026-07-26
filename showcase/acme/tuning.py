# showcase/acme/tuning.py
"""Every rate and duration the showcase uses, in one file.

Nothing outside this module hard-codes a probability, an interval, or a
sleep. Re-balancing the demo — faster orders, rarer declines, longer
picking — is an edit here and nowhere else.

Failure rates are the fraction of *domain identifiers* that draw a given
fault, not a per-execution coin flip: `simulate` hashes the id, so the same
order always draws the same faults.
"""

from __future__ import annotations

from typing import Final

from .simulate import WorkEnvelope

# --- Catalog and orders -------------------------------------------------

CATALOG_SIZE: Final[int] = 60
"""Products the `seed` scenario loads."""

CATALOG_STOCK_PER_SKU: Final[int] = 500
"""Units stocked per product — deep enough that a 10-minute run never drains it."""

DISCONTINUED_SKU_COUNT: Final[int] = 4
"""Products seeded with zero stock; the shortfall draw routes orders to them."""

MIN_LINES_PER_ORDER: Final[int] = 1
MAX_LINES_PER_ORDER: Final[int] = 3
MIN_QTY_PER_LINE: Final[int] = 1
MAX_QTY_PER_LINE: Final[int] = 4

MIN_PRICE_CENTS: Final[int] = 1_290
MAX_PRICE_CENTS: Final[int] = 14_900

# --- Scenario pacing ----------------------------------------------------

STEADY_MIN_INTERARRIVAL_SECONDS: Final[int] = 4
STEADY_MAX_INTERARRIVAL_SECONDS: Final[int] = 8
"""One order every 4-8 s: ~7.5 concurrent task slots of demand against the 12
executor children S7 documents, so steady stays smooth and every node is
catchable in RUNNING."""

# --- Failure rates ------------------------------------------------------

PSP_UNAVAILABLE_RATE: Final[float] = 0.20
"""Orders whose first authorization attempts hit an unreachable payment provider."""

PSP_FAILING_ATTEMPTS: Final[int] = 2
"""How many authorization attempts a PSP outage survives before it clears —
the retry policy below must allow at least this many."""

CARD_DECLINE_RATE: Final[float] = 0.08
"""Orders the issuer declines. Permanent: a dashboard retry declines again."""

STOCK_SHORTFALL_RATE: Final[float] = 0.05
"""Orders that include a discontinued SKU, so the reservation genuinely fails."""

INVOICE_HANG_RATE: Final[float] = 0.03
"""Orders whose invoice render stalls past `INVOICE_TIMEOUT_MS`."""

COURIER_FLAKE_RATE: Final[float] = 0.10
"""Orders whose first courier booking attempt hits a flaking carrier API."""

COURIER_FAILING_ATTEMPTS: Final[int] = 1

PROMOTION_BUNDLE_BUG_RATE: Final[float] = 0.04
"""Orders that trip the bundle-pricing division bug — an unmapped
ZeroDivisionError, wrapped as UNHANDLED_EXCEPTION."""

PROMOTION_SIZE_CODE_BUG_RATE: Final[float] = 0.04
"""Orders whose size code is missing — a KeyError the GLOBAL mapper turns
into DATA_CORRUPTION."""

LOYALTY_ENGINE_BUG_RATE: Final[float] = 0.02
"""Customers that trip the loyalty tier bug — an AttributeError surfaced
under the task's own `default_unhandled_error_code`."""

SUPPLIER_TIMEOUT_RATE: Final[float] = 0.25
"""Supplier feed pulls that time out; retried on a fixed schedule."""

# --- Retry policies -----------------------------------------------------

PSP_RETRY_BASE_SECONDS: Final[int] = 5
PSP_MAX_RETRIES: Final[int] = 4

COURIER_RETRY_BASE_SECONDS: Final[int] = 3
COURIER_MAX_RETRIES: Final[int] = 3

SUPPLIER_RETRY_INTERVALS_SECONDS: Final[list[int]] = [10, 30, 60]

CRASH_RETRY_INTERVALS_SECONDS: Final[list[int]] = [3, 10]
"""Backoff for tasks re-run after a worker crash. A `timeout_ms` kill breaks
the executor pool, so every task in flight on that worker is reported
WORKER_CRASHED; each of them is idempotent on replay and comes back through
this policy."""

# --- Work envelopes -----------------------------------------------------
# 2-6 s per node: long enough that RUNNING is catchable in the dashboard.

VALIDATE_ORDER_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=2_000, high_ms=3_500)
RESERVE_STOCK_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=2_000, high_ms=4_000)
RELEASE_STOCK_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=1_500, high_ms=3_000)
ALLOCATE_WAREHOUSE_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=2_000, high_ms=4_000)
AUTHORIZE_PAYMENT_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=2_500, high_ms=5_000)
CAPTURE_PAYMENT_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=2_000, high_ms=4_000)
REFUND_PAYMENT_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=2_000, high_ms=4_000)
PICK_PACK_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=3_000, high_ms=6_000)
GENERATE_INVOICE_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=2_000, high_ms=5_000)
BOOK_COURIER_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=2_500, high_ms=5_000)
PRINT_LABEL_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=2_000, high_ms=3_500)
TRACKING_SEED_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=2_000, high_ms=3_000)
SEND_ORDER_EMAIL_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=2_000, high_ms=3_500)
APPLY_PROMOTIONS_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=2_000, high_ms=4_000)
LOYALTY_POINTS_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=2_000, high_ms=3_500)
SUPPLIER_FEED_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=3_000, high_ms=6_000)
UPDATE_STOCK_LEVELS_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=2_000, high_ms=4_000)

INVOICE_TIMEOUT_MS: Final[int] = 8_000
"""`timeout_ms` on generate_invoice. Over-deadline renders are SIGKILLed."""

INVOICE_HANG_MS: Final[int] = 20_000
"""How long a stuck render would sleep — it never gets there."""

# --- Returns ------------------------------------------------------------

RETURN_SPAWN_EVERY: Final[int] = 6
"""Steady mode opens a return for one order in this many."""

RETURN_DAMAGE_RATE: Final[float] = 0.30
"""Returns whose inspection finds damage. `returns_review` runs under
`OnError.PAUSE`, so each one pauses its workflow for a human decision."""

RECEIVE_RETURN_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=2_000, high_ms=3_500)
INSPECT_ITEM_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=2_500, high_ms=5_000)
RESTOCK_OR_WRITEOFF_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=2_000, high_ms=4_000)

# --- Restock (quorum) ---------------------------------------------------

RESTOCK_SPAWN_EVERY: Final[int] = 20
"""Steady mode starts a `restock` run every this many orders, so the quorum
join is exercised without a scenario of its own."""

RESTOCK_MIN_SUCCESSFUL_FEEDS: Final[int] = 2
"""`min_success` on the aggregate node: two of three suppliers is enough."""

RESTOCK_UNITS_PER_SUPPLIER: Final[int] = 40
RESTOCK_SKUS_PER_SUPPLIER: Final[int] = 5

# --- Flash sale ---------------------------------------------------------

FLASH_SALE_SKUS: Final[int] = 6
FLASH_SALE_DISCOUNT_PERCENT: Final[int] = 30

CDN_REJECT_RATE: Final[float] = 0.35
ORIGIN_REJECT_RATE: Final[float] = 0.35
"""Publish targets fail independently; the SuccessPolicy treats either one
landing as success. The `flash-sale` scenario runs a second sale with a
campaign id engineered to miss both, which reports
WORKFLOW_SUCCESS_CASE_NOT_MET."""

SEARCH_PREWARM_FAIL_RATE: Final[float] = 0.50
"""`prewarm_search` is declared optional in the SuccessPolicy, so this rate
never affects whether the sale succeeded."""

EXPIRING_PRICE_SENDS: Final[int] = 80
PRICE_GOOD_UNTIL_SECONDS: Final[int] = 45
"""`update_price` is sent with `with_options(good_until=...)` into a queue that
cannot drain 80 sends in 45 s, so the tail expires rather than running late."""

PUBLISH_CDN_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=2_500, high_ms=4_500)
PUBLISH_ORIGIN_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=2_500, high_ms=4_500)
PREWARM_SEARCH_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=3_000, high_ms=5_000)
WARM_CACHE_EDGE_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=2_000, high_ms=3_500)
UPDATE_PRICE_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=2_000, high_ms=3_000)

# --- Catalog import -----------------------------------------------------

CATALOG_IMPORT_CHUNKS: Final[int] = 40
CATALOG_IMPORT_CHUNK_WORK: Final[WorkEnvelope] = WorkEnvelope(
    low_ms=7_000, high_ms=9_000,
)
"""40 chunks at ~8 s on a queue capped at 2: long enough to cancel mid-run and
watch pending nodes go SKIPPED while running ones drain."""

CATALOG_IMPORT_ROWS_PER_CHUNK: Final[int] = 500

# --- Analytics ----------------------------------------------------------

SALES_ROLLUP_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=3_000, high_ms=5_000)
ABANDONED_CART_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=2_500, high_ms=4_000)
RECONCILE_PAYMENTS_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=3_000, high_ms=5_000)
FLAKY_EXPORT_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=2_000, high_ms=4_000)

ABANDONED_CART_AGE_MINUTES: Final[int] = 15

CHAOS_EXPORT_CRASH_RATE: Final[float] = 0.50
"""`flaky_export` calls os._exit(1) in the child for this share of export ids.
The task never returns, so the worker reports WORKER_CRASHED and the retry
policy brings it back — a different export id on the retry would defeat the
demo, so the id is what the crash draw hashes."""

CHAOS_EXPORT_SPACING_SECONDS: Final[int] = 30
CHAOS_EXPORT_RETRY_INTERVALS_SECONDS: Final[list[int]] = [30, 60]
"""Chaos is spaced out on purpose, and this is not cosmetic.

A child that kills itself breaks the executor pool, and the worker replaces the
whole pool — twelve children, which takes a few seconds to warm. If another
self-killing task is dispatched *during* that warmup, the restart itself fails
and the worker stops fail-closed rather than running without an executor. That
is correct behaviour, and it is also how a chaos demo accidentally takes down
the thing it is meant to demonstrate recovering.

Spacing the sends by 30 s, and backing the retries off by 30 s and 60 s rather
than the 3 s and 10 s the ordinary crash policy uses, keeps at most one
self-kill in flight, so every restart completes and every crash is recovered."""

# --- Notifications ------------------------------------------------------

SEND_SHIPPING_SMS_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=2_000, high_ms=3_000)
MARKETING_BLAST_WORK: Final[WorkEnvelope] = WorkEnvelope(low_ms=4_000, high_ms=7_000)

MARKETING_BLAST_SEGMENTS: Final[int] = 40
"""Sent in one burst onto a queue capped at 3, which is what builds the deep
PENDING backlog the queue pivot and the task-cancel action are demonstrated on."""

MARKETING_SEGMENT_SIZE: Final[int] = 2_500

# --- Scenario volumes ---------------------------------------------------

RUSH_ORDER_COUNT: Final[int] = 50
RUSH_WINDOW_SECONDS: Final[int] = 30

PROBLEM_CHILD_RETURNS: Final[int] = 10
PROBLEM_CHILD_DECLINES: Final[int] = 8

CHAOS_EXPORT_COUNT: Final[int] = 4

# --- Schedules ----------------------------------------------------------

SUPPLIER_FEED_INTERVAL_SECONDS: Final[int] = 90
ABANDONED_CART_MINUTE: Final[int] = 5
"""HourlySchedule: every hour at :05."""

SALES_ROLLUP_HOUR: Final[int] = 3
"""DailySchedule: 03:00, so it will not fire during a demo."""

RECONCILE_HOUR_STEP: Final[int] = 4
RECONCILE_MINUTE: Final[int] = 15
"""CronSchedule: every 4 hours at :15, expressed with CronStep + CronValues."""

REGIONS: Final[list[str]] = ['eu-central', 'uk', 'turkiye', 'nordics']
"""Regions with their own rollup and cache-warm schedules."""

CACHE_WARM_INTERVAL_MINUTES: Final[int] = 5
SEARCH_PREWARM_INTERVAL_MINUTES: Final[int] = 10
RETENTION_AUDIT_DAYS: Final[int] = 30
PRICE_SYNC_MINUTE_STEP: Final[int] = 15

# --- Simulated fixtures -------------------------------------------------

COURIERS: Final[list[str]] = ['fleetline', 'northgate', 'palermo-express']
EXPRESS_RATE: Final[float] = 0.30
"""Orders shipped express — changes the child workflow's build parameters."""

WAREHOUSES: Final[list[str]] = ['LEI-1', 'ROT-2', 'IST-3']
PICK_STATIONS: Final[list[str]] = ['A1', 'A2', 'B1', 'B2', 'C1']
SUPPLIERS: Final[list[str]] = ['atlas-textiles', 'brera-knitwear', 'coastline-denim']

PRODUCT_LINES: Final[list[str]] = [
    'Oversized Tee',
    'Cropped Hoodie',
    'Wide-Leg Jean',
    'Ribbed Knit',
    'Poplin Shirt',
    'Cargo Skirt',
    'Puffer Vest',
    'Slip Dress',
]
PRODUCT_COLOURS: Final[list[str]] = [
    'Bone',
    'Ink',
    'Sage',
    'Rust',
    'Cobalt',
    'Ecru',
]
PRODUCT_CATEGORIES: Final[list[str]] = ['tops', 'bottoms', 'outerwear', 'dresses']
SIZE_CODES: Final[list[str]] = ['XS', 'S', 'M', 'L', 'XL']

CORRUPT_SIZE_CODE: Final[str] = 'XXL'
"""Size code the promotions engine has no multiplier for. Orders drawn by
`PROMOTION_SIZE_CODE_BUG_RATE` genuinely carry it, so the KeyError is a
property of the data, not a scripted raise."""

PROMOTION_CODES: Final[list[str]] = ['SPRING10', 'BUNDLE3', 'LOYAL15', 'FREESHIP']

BUNDLE_MIN_QUANTITY: Final[int] = 2
"""Units on one line before it counts toward a bundle."""

BUNDLE_PRICE_FLOOR_CENTS: Final[int] = 1_000
"""Bundled lines below this price earn no share of the bundle pot. An order
whose every bundled line is under the floor divides the pot by zero — the
bundle-pricing bug. `MIN_PRICE_CENTS` sits above the floor, so only the
clearance-priced orders below reach it."""

BUNDLE_POT_DIVISOR: Final[int] = 10

CLEARANCE_PRICE_CENTS: Final[int] = 690
"""Markdown price written onto the lines of orders drawn by
`PROMOTION_BUNDLE_BUG_RATE`."""

LOYALTY_POINTS_PER_EURO: Final[int] = 2
LOYALTY_LIFETIME_MAX: Final[int] = 9_000
"""Lifetime points a normal customer draws — below the platinum threshold."""

LOYALTY_LIFETIME_BUG_MIN: Final[int] = 10_000
LOYALTY_LIFETIME_BUG_MAX: Final[int] = 40_000
"""Range drawn by customers who reach platinum, the one tier whose table row
is a bare label instead of a tier object."""
