# showcase/acme/scenarios/__init__.py
"""Shared scenario helpers: order building, and the pointers each run prints.

Orders are built, not invented. Every choice — customer, lines, quantities,
sizes, prices — is a stable hash of the order id, so re-running a scenario
against the same ids produces the same orders, and an order that failed once
fails the same way when you retry it from the dashboard.

Three of those choices deliberately produce difficult data:

  * `STOCK_SHORTFALL_RATE` puts a discontinued SKU on the order, so the
    reservation genuinely fails against the stock row;
  * `PROMOTION_BUNDLE_BUG_RATE` marks the order down to clearance prices,
    which is the shape that trips the bundle-pricing division;
  * `PROMOTION_SIZE_CODE_BUG_RATE` writes a size code the promotions engine
    has no multiplier for.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from typing import Final

from horsies import Err, Ok

from .. import simulate, store, tuning
from ..domain import CatalogEntry, Order, OrderLine
from ..store import StoreError, StoreResult
from ..workflows.returns_review import build_returns_review

WEB_BASE_URL: Final[str] = os.environ.get('ACME_WEB_URL', 'http://127.0.0.1:8600')
"""Where `horsies web` listens; scenarios print their links against it. Set
`ACME_WEB_URL` when the dashboard runs on another host or port."""


def say(text: str = '') -> None:
    """Print one line, flushed.

    Scenarios are watched live and are usually piped into a terminal
    multiplexer or a process manager, where Python block-buffers stdout and a
    running scenario looks like a hung one.
    """
    print(text, flush=True)


def heading(text: str) -> None:
    """Print a section heading."""
    say(f'\n{text}\n{"-" * len(text)}')


def bullet(text: str) -> None:
    """Print one line of guidance."""
    say(f'  {text}')


def _customer_id(order_id: str) -> str:
    return f'CUS-{simulate.integer(1, 400, order_id, "customer"):04d}'


def _size_code(order_id: str, line_no: int, *, corrupt: bool) -> str:
    if corrupt and line_no == 1:
        return tuning.CORRUPT_SIZE_CODE
    return simulate.choice(tuning.SIZE_CODES, order_id, 'size', str(line_no))


def build_order(order_id: str, catalog: Sequence[CatalogEntry]) -> Order:
    """Shape one order from the catalog, deterministically from its id."""
    in_stock = [entry for entry in catalog if entry.stock.available > 0]
    discontinued = [entry for entry in catalog if entry.stock.on_hand == 0]
    if not in_stock:
        raise ValueError('catalog has no sellable stock — run the seed scenario')

    line_count = min(
        simulate.integer(
            tuning.MIN_LINES_PER_ORDER,
            tuning.MAX_LINES_PER_ORDER,
            order_id,
            'lines',
        ),
        len(in_stock),
    )
    picked = simulate.sample(in_stock, line_count, order_id, 'skus')
    if discontinued and simulate.draw(tuning.STOCK_SHORTFALL_RATE, order_id, 'shortfall'):
        picked[0] = simulate.choice(discontinued, order_id, 'discontinued')

    clearance = simulate.draw(tuning.PROMOTION_BUNDLE_BUG_RATE, order_id, 'bundle-bug')
    corrupt_size = simulate.draw(
        tuning.PROMOTION_SIZE_CODE_BUG_RATE, order_id, 'size-bug',
    )

    lines: list[OrderLine] = []
    for index, entry in enumerate(picked, start=1):
        quantity = (
            tuning.BUNDLE_MIN_QUANTITY
            if clearance
            else simulate.integer(
                tuning.MIN_QTY_PER_LINE,
                tuning.MAX_QTY_PER_LINE,
                order_id,
                'qty',
                str(index),
            )
        )
        lines.append(
            OrderLine(
                line_no=index,
                sku=entry.product.sku,
                size_code=_size_code(order_id, index, corrupt=corrupt_size),
                quantity=quantity,
                unit_price_cents=(
                    tuning.CLEARANCE_PRICE_CENTS if clearance else entry.product.price_cents
                ),
            ),
        )

    return Order(
        order_id=order_id,
        customer_id=_customer_id(order_id),
        status='placed',
        total_cents=sum(line.line_total_cents for line in lines),
        lines=lines,
        created_at=datetime.now(timezone.utc),
    )


def place_order(catalog: Sequence[CatalogEntry]) -> StoreResult[Order]:
    """Take the next order number, build that order, and store it."""
    match store.next_order_number():
        case Err(error):
            return Err(error)
        case Ok(number):
            order_id = f'ACME-{number:05d}'

    order = build_order(order_id, catalog)

    match store.insert_order(order):
        case Err(error):
            return Err(error)
        case Ok(_):
            return Ok(order)


def reserve_order_id(
    wanted: Callable[[str], bool],
    *,
    limit: int = 500,
) -> StoreResult[str | None]:
    """Advance the order sequence until an id whose draws match `wanted`.

    Outcomes are a stable hash of the order id, so a scenario that needs a
    declined payment cannot force one onto an arbitrary order — it has to go
    find an id the issuer will decline. Burning sequence numbers is the honest
    way to do that: the orders that get placed are ordinary orders, and their
    failures are the ones their ids always had.
    """
    for _attempt in range(limit):
        match store.next_order_number():
            case Err(error):
                return Err(error)
            case Ok(number):
                order_id = f'ACME-{number:05d}'
        if wanted(order_id):
            return Ok(order_id)
    return Ok(None)


def store_order(order_id: str, catalog: Sequence[CatalogEntry]) -> StoreResult[Order]:
    """Build and persist an order under an id the caller already reserved."""
    order = build_order(order_id, catalog)
    match store.insert_order(order):
        case Err(error):
            return Err(error)
        case Ok(_):
            return Ok(order)


def open_return_for(order_id: str, sku: str, quantity: int) -> StoreResult[str]:
    """Reserve a return id and start a `returns_review` run for it.

    Returns the workflow id so the caller can print a link, or an error if the
    sequence or the workflow start failed.
    """
    match store.next_return_number():
        case Err(error):
            return Err(error)
        case Ok(number):
            return_id = f'RET-{number:05d}'

    match build_returns_review(
        return_id=return_id,
        order_id=order_id,
        sku=sku,
        quantity=quantity,
    ).start():
        case Ok(handle):
            return Ok(f'{return_id} {handle.workflow_id}')
        case Err(error):
            return Err(
                StoreError(
                    operation='start returns_review',
                    message=f'[{error.code}] {error.message}',
                ),
            )


def will_pause(return_id: str) -> bool:
    """Whether this return's inspection will find damage, and so pause its run.

    Knowable before the workflow starts, because the draw is a stable hash of
    the return id — which is what lets a scenario tell you in advance which
    runs are going to need you.
    """
    return simulate.draw(tuning.RETURN_DAMAGE_RATE, return_id, 'damage')


def load_catalog_or_explain() -> StoreResult[list[CatalogEntry]]:
    """Read the catalog, or explain that the seed scenario has not run."""
    match store.list_catalog():
        case Err(error):
            say(f'cannot read the catalog: {error.operation} — {error.message}')
            say('run the seed scenario first')
            return Err(error)
        case Ok(catalog):
            if not catalog:
                say('the catalog is empty.')
                say('run the seed scenario first')
            return Ok(catalog)
