# showcase/hemline/tasks/promotions.py
"""The promotions engine — and the three ways an exception reaches the dashboard.

Nothing here raises on purpose. Both tasks contain real edge-case bugs, and
the orders that trip them genuinely carry the data that does it: a clearance
order whose every bundled line sits under the bundle price floor divides by
zero, and an order line whose size code is missing from the multiplier table
raises `KeyError` on lookup.

What the showcase is demonstrating is what horsies does with them:

  * `ZeroDivisionError` has no mapper entry anywhere, so it is intercepted and
    reported as `UNHANDLED_EXCEPTION` with the exception type, message, and
    traceback carried inside `TaskError.exception`. The crash is a data
    structure you can open in the dashboard — nothing raised into the void.
  * `KeyError` is mapped globally in `app.py` to `DATA_CORRUPTION`, so the same
    interception reports a domain code instead.
  * `compute_loyalty_points` sets `default_unhandled_error_code`, so its own
    unmapped `AttributeError` surfaces as `LOYALTY_ENGINE_BUG` — the team's
    vocabulary rather than the generic code.
"""

from __future__ import annotations

from typing import Any, Final

from pydantic import BaseModel

from horsies import (
    Err,
    Ok,
    OperationalErrorCode,
    RetryPolicy,
    TaskError,
    TaskResult,
)

from .. import simulate, store, tuning
from ..app import QUEUE_ANALYTICS, QUEUE_FULFILLMENT, app
from ..domain import (
    CDN_REJECTED,
    LOYALTY_ENGINE_BUG,
    ORDER_NOT_FOUND,
    ORIGIN_REJECTED,
    SEARCH_INDEX_STALE,
    UNKNOWN_SKU,
    CacheWarm,
    LoyaltyPoints,
    Order,
    PriceUpdate,
    PricePush,
    PromotionOutcome,
    SearchPrewarm,
)
from . import store_failure

_SIZE_MULTIPLIERS: Final[dict[str, int]] = {
    'XS': 90,
    'S': 95,
    'M': 100,
    'L': 105,
    'XL': 110,
}
"""Per-size price index, in percent. `tuning.CORRUPT_SIZE_CODE` is absent."""

class LoyaltyTier(BaseModel):
    """A tier's label and its points multiplier, in percent."""

    label: str
    multiplier: int


_TIER_TABLE: Final[dict[str, Any]] = {
    'bronze': LoyaltyTier(label='bronze', multiplier=100),
    'silver': LoyaltyTier(label='silver', multiplier=125),
    'gold': LoyaltyTier(label='gold', multiplier=150),
    # Authored later, by hand, as a bare label. `dict[str, Any]` is what lets
    # it through: the tier table is the engine's untyped boundary, and this is
    # the row that dereferences into an AttributeError.
    'platinum': 'platinum',
}

_TIER_THRESHOLDS: Final[list[tuple[int, str]]] = [
    (10_000, 'platinum'),
    (5_000, 'gold'),
    (1_000, 'silver'),
    (0, 'bronze'),
]


def _size_weighted_cents(order: Order) -> int:
    """Order value re-weighted by each line's size index."""
    return sum(
        line.line_total_cents * _SIZE_MULTIPLIERS[line.size_code] // 100
        for line in order.lines
    )


def _bundle_discount_cents(order: Order) -> int:
    """Split the bundle pot evenly across the units that earn a share."""
    bundled = [
        line for line in order.lines if line.quantity >= tuning.BUNDLE_MIN_QUANTITY
    ]
    if not bundled:
        return 0
    pot_cents = sum(line.line_total_cents for line in bundled) // tuning.BUNDLE_POT_DIVISOR
    earning_units = sum(
        line.quantity
        for line in bundled
        if line.unit_price_cents >= tuning.BUNDLE_PRICE_FLOOR_CENTS
    )
    return pot_cents // earning_units


@app.task(
    'apply_promotions',
    queue_name=QUEUE_FULFILLMENT,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def apply_promotions(*, order_id: str) -> TaskResult[PromotionOutcome, TaskError]:
    """Price an order's promotions.

    Sent standalone per order, not as a workflow node, so its failures land in
    the task list beside the workflow runs. `UNHANDLED_EXCEPTION` is absent
    from `auto_retry_for` on purpose: a crash that reproduces every time is
    worth looking at, not worth retrying.
    """
    simulate.perform(tuning.APPLY_PROMOTIONS_WORK, order_id, 'promotions')

    match store.get_order(order_id):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(order):
            if order is None:
                return TaskResult(
                    err=TaskError(
                        error_code=ORDER_NOT_FOUND,
                        message=f'no order {order_id} to price',
                        data={'order_id': order_id},
                    ),
                )
            priced = order

    weighted_cents = _size_weighted_cents(priced)
    discount_cents = _bundle_discount_cents(priced)
    codes = [
        code
        for code in tuning.PROMOTION_CODES
        if simulate.draw(0.5, order_id, 'promo', code)
    ]
    return TaskResult(
        ok=PromotionOutcome(
            order_id=order_id,
            discount_cents=min(discount_cents, weighted_cents),
            applied_codes=codes,
        ),
    )


def _tier_name(lifetime_points: int) -> str:
    """The tier a lifetime-point total earns."""
    for threshold, name in _TIER_THRESHOLDS:
        if lifetime_points >= threshold:
            return name
    return _TIER_THRESHOLDS[-1][1]


@app.task(
    'compute_loyalty_points',
    queue_name=QUEUE_ANALYTICS,
    default_unhandled_error_code=LOYALTY_ENGINE_BUG,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def compute_loyalty_points(
    *,
    customer_id: str,
    order_id: str,
) -> TaskResult[LoyaltyPoints, TaskError]:
    """Award points for an order, at the customer's tier multiplier."""
    simulate.perform(tuning.LOYALTY_POINTS_WORK, customer_id, order_id, 'loyalty')

    match store.get_order(order_id):
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(order):
            if order is None:
                return TaskResult(
                    err=TaskError(
                        error_code=ORDER_NOT_FOUND,
                        message=f'no order {order_id} to award points for',
                        data={'order_id': order_id},
                    ),
                )
            awarded = order

    tier = _TIER_TABLE[_tier_name(_lifetime_points(customer_id))]
    base_points = awarded.total_cents // 100 * tuning.LOYALTY_POINTS_PER_EURO
    return TaskResult(
        ok=LoyaltyPoints(
            customer_id=customer_id,
            order_id=order_id,
            points=base_points * tier.multiplier // 100,
            tier=tier.label,
        ),
    )


@app.task(
    'publish_cdn',
    queue_name=QUEUE_FULFILLMENT,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def publish_cdn(*, campaign_id: str, sku: str) -> TaskResult[PricePush, TaskError]:
    """Push sale prices to the CDN edge. One of two ways a sale can succeed."""
    simulate.perform(tuning.PUBLISH_CDN_WORK, campaign_id, 'cdn')

    if simulate.draw(tuning.CDN_REJECT_RATE, campaign_id, 'cdn-reject'):
        return TaskResult(
            err=TaskError(
                error_code=CDN_REJECTED,
                message=f'CDN refused the {campaign_id} price bundle',
                data={'campaign_id': campaign_id},
            ),
        )
    return TaskResult(
        ok=PricePush(sku=sku, price_cents=_sale_price(sku), target='cdn'),
    )


@app.task(
    'publish_origin',
    queue_name=QUEUE_FULFILLMENT,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def publish_origin(*, campaign_id: str, sku: str) -> TaskResult[PricePush, TaskError]:
    """Push sale prices to origin. The other way a sale can succeed."""
    simulate.perform(tuning.PUBLISH_ORIGIN_WORK, campaign_id, 'origin')

    if simulate.draw(tuning.ORIGIN_REJECT_RATE, campaign_id, 'origin-reject'):
        return TaskResult(
            err=TaskError(
                error_code=ORIGIN_REJECTED,
                message=f'origin refused the {campaign_id} price bundle',
                data={'campaign_id': campaign_id},
            ),
        )
    return TaskResult(
        ok=PricePush(sku=sku, price_cents=_sale_price(sku), target='origin'),
    )


@app.task(
    'prewarm_search',
    queue_name=QUEUE_ANALYTICS,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def prewarm_search(*, campaign_id: str) -> TaskResult[SearchPrewarm, TaskError]:
    """Rebuild the search index for the sale.

    Declared `optional` in the workflow's SuccessPolicy, so it is excluded from
    failure accounting entirely — it fails half the time and the sale still
    completes.
    """
    simulate.perform(tuning.PREWARM_SEARCH_WORK, campaign_id, 'search')

    if simulate.draw(tuning.SEARCH_PREWARM_FAIL_RATE, campaign_id, 'search-fail'):
        return TaskResult(
            err=TaskError(
                error_code=SEARCH_INDEX_STALE,
                message=f'search index for {campaign_id} did not finish rebuilding',
                data={'campaign_id': campaign_id},
            ),
        )
    return TaskResult(
        ok=SearchPrewarm(
            documents=simulate.integer(5_000, 50_000, campaign_id, 'docs'),
            index_name=f'catalog-{campaign_id}',
        ),
    )


@app.task(
    'warm_cache_edge',
    queue_name=QUEUE_FULFILLMENT,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def warm_cache_edge(*, campaign_id: str) -> TaskResult[CacheWarm, TaskError]:
    """Warm the edge cache as soon as *either* publish lands.

    Its node uses `join='any'`, so it fires on the first of the two publishes
    to complete instead of waiting for both.
    """
    simulate.perform(tuning.WARM_CACHE_EDGE_WORK, campaign_id, 'warm')

    return TaskResult(
        ok=CacheWarm(
            target='edge',
            keys_warmed=simulate.integer(200, 2_000, campaign_id, 'keys'),
        ),
    )


@app.task(
    'update_price',
    queue_name=QUEUE_FULFILLMENT,
    retry_policy=RetryPolicy.fixed(
        tuning.CRASH_RETRY_INTERVALS_SECONDS,
        auto_retry_for=[OperationalErrorCode.WORKER_CRASHED],
    ),
)
def update_price(*, sku: str, campaign_id: str) -> TaskResult[PriceUpdate, TaskError]:
    """Apply one sale price.

    Sent in bulk with `with_options(good_until=...)`. The queue cannot drain
    the burst inside the deadline, so the tail is never claimed and the reaper
    reports it EXPIRED — which is a different outcome from failing, and the
    dashboard shows it as one.
    """
    simulate.perform(tuning.UPDATE_PRICE_WORK, sku, campaign_id, 'price')

    match store.list_catalog():
        case Err(error):
            return TaskResult(err=store_failure(error))
        case Ok(catalog):
            listed = [entry for entry in catalog if entry.product.sku == sku]

    if not listed:
        return TaskResult(
            err=TaskError(
                error_code=UNKNOWN_SKU,
                message=f'{sku} is not in the catalog',
                data={'sku': sku},
            ),
        )
    was_cents = listed[0].product.price_cents
    return TaskResult(
        ok=PriceUpdate(sku=sku, was_cents=was_cents, now_cents=_sale_price(sku)),
    )


def _sale_price(sku: str) -> int:
    """Flash-sale price for a SKU, from its stable base price."""
    base = simulate.integer(
        tuning.MIN_PRICE_CENTS, tuning.MAX_PRICE_CENTS, sku, 'price',
    )
    return base * (100 - tuning.FLASH_SALE_DISCOUNT_PERCENT) // 100


def _lifetime_points(customer_id: str) -> int:
    """Points the customer has collected so far."""
    if simulate.draw(tuning.LOYALTY_ENGINE_BUG_RATE, customer_id, 'lifetime-bug'):
        return simulate.integer(
            tuning.LOYALTY_LIFETIME_BUG_MIN,
            tuning.LOYALTY_LIFETIME_BUG_MAX,
            customer_id,
            'lifetime',
        )
    return simulate.integer(0, tuning.LOYALTY_LIFETIME_MAX, customer_id, 'lifetime')
