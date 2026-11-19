# showcase/acme/scenarios/problem_child.py
"""Everything that needs a human, in one go.

    uv run python -m showcase.acme.scenarios problem-child

Three kinds of trouble, deliberately concentrated:

  * returns whose inspection finds damage, so their workflows PAUSE and wait
    for you to resume or cancel them;
  * orders whose cards will be declined, which is retry fodder — retrying a
    declined payment from the dashboard declines it again, because the outcome
    belongs to the order id;
  * one order placed against a discontinued SKU, so the reservation fails and
    the skip cascade is visible in the graph.

None of it is forced. Outcomes are a stable hash of the order id, so the
scenario advances the order sequence until it finds ids that already have the
outcome it wants, then places ordinary orders under them.
"""

from __future__ import annotations

from horsies import Err, Ok

from .. import simulate, store, tuning
from ..settings import DATABASE
from . import (
    WEB_BASE_URL,
    bullet,
    heading,
    load_catalog_or_explain,
    open_return_for,
    reserve_order_id,
    say,
    store_order,
    will_pause,
)
from .steady import send_standalone, start_fulfillment


def run() -> int:
    """Create the pile of things that need attention. Returns an exit code."""
    heading('Acme Clothing — problem child')
    say(f'database: {DATABASE.database_name}  (resolved from {DATABASE.source})')

    match load_catalog_or_explain():
        case Err(_):
            return 1
        case Ok(catalog):
            if not catalog:
                return 1
            entries = catalog

    heading(f'{tuning.PROBLEM_CHILD_DECLINES} orders that will be declined')
    for _index in range(tuning.PROBLEM_CHILD_DECLINES):
        match reserve_order_id(
            lambda order_id: simulate.draw(tuning.CARD_DECLINE_RATE, order_id, 'card'),
        ):
            case Err(error):
                say(f'could not reserve an order id: {error.message}')
                return 1
            case Ok(order_id):
                if order_id is None:
                    say('no order id in range draws a decline — check the rate')
                    return 1
        match store_order(order_id, entries):
            case Err(error):
                say(f'could not store {order_id}: {error.message}')
                return 1
            case Ok(order):
                start_fulfillment(order)
                send_standalone(order)

    heading('one order against a discontinued SKU')
    match reserve_order_id(
        lambda order_id: simulate.draw(
            tuning.STOCK_SHORTFALL_RATE, order_id, 'shortfall',
        ),
    ):
        case Err(error):
            say(f'could not reserve an order id: {error.message}')
            return 1
        case Ok(order_id):
            if order_id is None:
                say('no order id in range draws a shortfall — check the rate')
                return 1
    match store_order(order_id, entries):
        case Err(error):
            say(f'could not store {order_id}: {error.message}')
            return 1
        case Ok(order):
            start_fulfillment(order)

    heading(f'{tuning.PROBLEM_CHILD_RETURNS} returns')
    match store.list_returnable_orders(tuning.PROBLEM_CHILD_RETURNS):
        case Err(error):
            say(f'could not list returnable orders: {error.message}')
            return 1
        case Ok(returnable):
            if not returnable:
                say('no captured orders to return yet — run steady for a few minutes first')
            paused = 0
            for order_id, sku, quantity in returnable:
                match open_return_for(order_id, sku, quantity):
                    case Err(error):
                        say(f'return on {order_id} failed: {error.message}')
                    case Ok(started):
                        return_id, _, workflow_id = started.partition(' ')
                        pauses = will_pause(return_id)
                        paused += 1 if pauses else 0
                        say(
                            f'{return_id}  {"WILL PAUSE" if pauses else "clean"}  ->  '
                            f'{WEB_BASE_URL}/workflows?run={workflow_id}'
                        )

    heading('what to do')
    bullet(f'{WEB_BASE_URL}/workflows?status=PAUSED  the runs waiting on you')
    bullet('  open one, read the DAMAGED_ITEM error on inspect_item, then Resume')
    bullet('  resuming runs restock_or_writeoff on the damaged branch and writes it off')
    bullet('  or Cancel — leaving the return open is a legitimate answer too')
    bullet(f'{WEB_BASE_URL}/?error_code=CARD_DECLINED  retry one; it declines again')
    bullet(f'{WEB_BASE_URL}/?error_code=INSUFFICIENT_STOCK  the skip cascade')
    return 0
