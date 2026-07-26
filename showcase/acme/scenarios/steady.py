# showcase/acme/scenarios/steady.py
"""Place an order every few seconds until Ctrl-C — the "watch the dashboard" mode.

    uv run python -m showcase.acme.scenarios steady

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
from ..workflows.restock import build_restock
from . import (
    WEB_BASE_URL,
    bullet,
    heading,
    load_catalog_or_explain,
    open_return_for,
    place_order,
    say,
    will_pause,
)


def start_fulfillment(order: Order) -> None:
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


def send_standalone(order: Order) -> None:
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


def spawn_return(order: Order) -> None:
    """Open a return against a delivered order and start its review run.

    One order in `RETURN_SPAWN_EVERY` gets one. Whether the inspection finds
    damage — and so whether the run pauses for you — is knowable before it
    starts, so the link is labelled accordingly.
    """
    line = order.lines[0]
    match open_return_for(order.order_id, line.sku, line.quantity):
        case Err(error):
            say(f'{order.order_id}  return failed: {error.operation} — {error.message}')
        case Ok(started):
            return_id, _, workflow_id = started.partition(' ')
            marker = 'WILL PAUSE' if will_pause(return_id) else 'clean'
            say(
                f'{return_id}  return on {order.order_id} ({marker})  ->  '
                f'{WEB_BASE_URL}/workflows?run={workflow_id}'
            )


def spawn_restock() -> None:
    """Start a supplier restock — the quorum join, in the default demo mode.

    Three feeds, two of which are enough. Whichever supplier times out is
    absent from the aggregate's context, and the run completes anyway with a
    failed branch still drawn in the graph.
    """
    match build_restock(suppliers=list(tuning.SUPPLIERS)).start():
        case Err(error):
            say(f'restock start failed: [{error.code}] {error.message}')
        case Ok(handle):
            say(
                f'restock   {len(tuning.SUPPLIERS)} feeds, quorum '
                f'{tuning.RESTOCK_MIN_SUCCESSFUL_FEEDS}  ->  '
                f'{WEB_BASE_URL}/workflows?run={handle.workflow_id}'
            )


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
    heading('Acme Clothing — steady')
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
                    start_fulfillment(order)
                    send_standalone(order)
                    if placed % tuning.RETURN_SPAWN_EVERY == 0:
                        spawn_return(order)
                    if placed % tuning.RESTOCK_SPAWN_EVERY == 0:
                        spawn_restock()
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
