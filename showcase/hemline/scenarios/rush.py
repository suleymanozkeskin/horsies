# showcase/hemline/scenarios/rush.py
"""Fifty orders in thirty seconds — what saturation looks like.

    uv run python -m showcase.hemline.scenarios rush

Steady mode runs below capacity on purpose. This one runs well above it: the
arrival rate is roughly ten times what twelve executor children can absorb, so
the per-queue caps start to bite and the backlog becomes visible.

Watch the queue pivot while it drains. `payments` is priority 1 and capped at
4, so authorizations keep moving; `notifications` is priority 50 and capped at
3, so order emails queue up behind everything else. That ordering is the whole
point of per-queue priority — the slow, low-value work cannot crowd out the
work that holds a customer's money.
"""

from __future__ import annotations

import time

from horsies import Err, Ok

from .. import tuning
from ..settings import DATABASE
from . import (
    WEB_BASE_URL,
    bullet,
    heading,
    load_catalog_or_explain,
    place_order,
    say,
)
from .steady import send_standalone, start_fulfillment


def run() -> int:
    """Place a burst of orders. Returns a process exit code."""
    heading('Hemline — rush')
    say(f'database: {DATABASE.database_name}  (resolved from {DATABASE.source})')

    match load_catalog_or_explain():
        case Err(_):
            return 1
        case Ok(catalog):
            if not catalog:
                return 1
            entries = catalog

    interval = tuning.RUSH_WINDOW_SECONDS / tuning.RUSH_ORDER_COUNT
    heading('what to watch')
    bullet(f'{WEB_BASE_URL}/?view=queue        the backlog, pivoted by queue')
    bullet(f'{WEB_BASE_URL}/?status=PENDING    what has not been claimed yet')
    bullet(f'{WEB_BASE_URL}/?queue=payments    still moving, because it is priority 1')
    bullet(f'{WEB_BASE_URL}/?queue=notifications  the deepest queue, capped at 3')
    say()

    heading(f'{tuning.RUSH_ORDER_COUNT} orders, one every {interval:.1f} s')
    placed = 0
    for _index in range(tuning.RUSH_ORDER_COUNT):
        match place_order(entries):
            case Err(error):
                say(f'could not place an order: {error.operation} — {error.message}')
                return 1
            case Ok(order):
                placed += 1
                start_fulfillment(order)
                send_standalone(order)
        time.sleep(interval)

    say()
    say(f'placed {placed} orders in about {tuning.RUSH_WINDOW_SECONDS} s')
    bullet('the backlog drains on its own — watch the queue pivot empty')
    return 0
