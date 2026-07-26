# showcase/hemline/tuning.py
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

# --- Schedules ----------------------------------------------------------

SUPPLIER_FEED_INTERVAL_SECONDS: Final[int] = 90

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
