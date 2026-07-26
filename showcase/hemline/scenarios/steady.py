# showcase/hemline/scenarios/steady.py
"""Place an order every few seconds until Ctrl-C — the "watch the dashboard" mode.

    uv run python -m showcase.hemline.scenarios steady

Each order starts one `order_fulfillment` run and two standalone sends. Every
failure the demo can produce appears on its own within a few minutes: retried
authorizations, declined cards, out-of-stock skips, a stalled invoice, and the
two promotions crashes.
"""

from __future__ import annotations

import time
from collections.abc import Sequence

from horsies import Err, Ok

from .. import simulate, tuning
from ..domain import CatalogEntry, Order
from ..settings import DATABASE
from ..tasks.promotions import apply_promotions, compute_loyalty_points
from ..workflows.order_fulfillment import build_order_fulfillment
from . import (
    WEB_BASE_URL,
    bullet,
    heading,
    load_catalog_or_explain,
    place_order,
    say,
)


def _start_fulfillment(order: Order) -> None:
    """Start the flagship workflow for one order and print its deep link.

    `resend_on_transient_err` is on, so a transient enqueue failure has
    already been retried by the time an `Err` arrives here.
    """
    match build_order_fulfillment(order=order).start():
        case Ok(handle):
            say(
                f'{order.order_id}  {len(order.lines)} line(s), '
                f'{order.total_cents / 100:.2f} EUR  ->  '
                f'{WEB_BASE_URL}/workflows?run={handle.workflow_id}'
            )
        case Err(error):
            say(f'{order.order_id}  workflow start failed: [{error.code}] {error.message}')


def _send_standalone(order: Order) -> None:
    """Send the two per-order tasks that are not part of the workflow."""
    match apply_promotions.send(order_id=order.order_id):
        case Ok(_):
            pass
        case Err(error):
            say(f'{order.order_id}  apply_promotions send failed: [{error.code}]')

    match compute_loyalty_points.send(
        customer_id=order.customer_id,
        order_id=order.order_id,
    ):
        case Ok(_):
            pass
        case Err(error):
            say(f'{order.order_id}  compute_loyalty_points send failed: [{error.code}]')


def _what_to_watch() -> None:
    heading('what to watch')
    bullet(f'{WEB_BASE_URL}/workflows              every run, live')
    bullet(f'{WEB_BASE_URL}/?retried=true          authorizations that survived a PSP outage')
    bullet(f'{WEB_BASE_URL}/?error_code=CARD_DECLINED       declines — retry one, it declines again')
    bullet(f'{WEB_BASE_URL}/?error_code=INSUFFICIENT_STOCK  the skip cascade in the graph view')
    bullet(f'{WEB_BASE_URL}/?error_code=UNHANDLED_EXCEPTION the bundle-pricing crash, as data')
    bullet(f'{WEB_BASE_URL}/?error_code=DATA_CORRUPTION     the same interception, mapped globally')
    bullet(f'{WEB_BASE_URL}/?error_code=LOYALTY_ENGINE_BUG  and under a task-local code')
    bullet(f'{WEB_BASE_URL}/?error_code=TASK_TIMEOUT        a stalled invoice render')
    bullet(f'{WEB_BASE_URL}/workers                CPU and memory, snapshotted every 10 s')
    say()
    bullet('pause a RUNNING workflow from its run page, then resume it')
    bullet('Ctrl-C stops placing orders; the ones already running finish')


def run() -> int:
    """Place orders until interrupted. Returns a process exit code."""
    heading('Hemline — steady')
    say(f'database: {DATABASE.database_name}  (resolved from {DATABASE.source})')

    match load_catalog_or_explain():
        case Err(_):
            return 1
        case Ok(catalog):
            if not catalog:
                return 1
            entries: Sequence[CatalogEntry] = catalog

    _what_to_watch()
    heading('orders')

    placed = 0
    try:
        while True:
            match place_order(entries):
                case Err(error):
                    say(f'could not place an order: {error.operation} — {error.message}')
                    return 1
                case Ok(order):
                    placed += 1
                    _start_fulfillment(order)
                    _send_standalone(order)
            time.sleep(
                simulate.integer(
                    tuning.STEADY_MIN_INTERARRIVAL_SECONDS,
                    tuning.STEADY_MAX_INTERARRIVAL_SECONDS,
                    order.order_id,
                    'interarrival',
                ),
            )
    except KeyboardInterrupt:
        say(f'\nstopped after placing {placed} orders')
        return 0
