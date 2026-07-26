# showcase/acme/scenarios/flash_sale.py
"""Two sales and a burst of expiring price updates.

    uv run python -m showcase.acme.scenarios flash-sale

The first campaign is an ordinary one: at least one publish target accepts it,
so the `SuccessPolicy` is satisfied and the run COMPLETES — even though the
other target may have failed and `prewarm_search` (declared optional) probably
did too.

The second is chosen so that *both* publishes fail. No SuccessCase can be
satisfied, and the run fails with `WORKFLOW_SUCCESS_CASE_NOT_MET` — which is a
different and more useful answer than "a task failed". The campaign id is not
rigged: the scenario searches for an id whose draws already reject at both
targets.

Then 80 `update_price` sends go out with a 45-second deadline onto a queue that
cannot drain them in time. The tail is never claimed and is reported EXPIRED,
which is not the same as failing — nothing went wrong, the work simply stopped
being worth doing.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from horsies import Err, Ok

from .. import simulate, store, tuning
from ..settings import DATABASE
from ..tasks.promotions import update_price
from ..workflows.flash_sale import build_flash_sale
from . import WEB_BASE_URL, bullet, heading, say


def _campaign_that_fails_both(limit: int = 500) -> str | None:
    """Find a campaign id whose CDN and origin publishes both reject."""
    for index in range(limit):
        campaign_id = f'FLASH-MISS-{index:04d}'
        cdn_fails = simulate.draw(tuning.CDN_REJECT_RATE, campaign_id, 'cdn-reject')
        origin_fails = simulate.draw(
            tuning.ORIGIN_REJECT_RATE, campaign_id, 'origin-reject',
        )
        if cdn_fails and origin_fails:
            return campaign_id
    return None


def _campaign_that_lands(limit: int = 500) -> str | None:
    """Find a campaign id where at least one publish target accepts."""
    for index in range(limit):
        campaign_id = f'FLASH-{index:04d}'
        cdn_fails = simulate.draw(tuning.CDN_REJECT_RATE, campaign_id, 'cdn-reject')
        origin_fails = simulate.draw(
            tuning.ORIGIN_REJECT_RATE, campaign_id, 'origin-reject',
        )
        if not (cdn_fails and origin_fails):
            return campaign_id
    return None


def _start_sale(campaign_id: str, sku: str, label: str) -> None:
    """Start one flash-sale run and print its link."""
    match build_flash_sale(campaign_id=campaign_id, sku=sku).start():
        case Err(error):
            say(f'{campaign_id}: start failed [{error.code}] {error.message}')
        case Ok(handle):
            say(
                f'{campaign_id}  ({label})  ->  '
                f'{WEB_BASE_URL}/workflows?run={handle.workflow_id}'
            )


def run() -> int:
    """Run both sales and the expiring price burst. Returns an exit code."""
    heading('Acme Clothing — flash sale')
    say(f'database: {DATABASE.database_name}  (resolved from {DATABASE.source})')

    match store.list_catalog():
        case Err(error):
            say(f'cannot read the catalog: {error.message}')
            return 1
        case Ok(catalog):
            if not catalog:
                say('the catalog is empty — run the seed scenario first')
                return 1
            skus = [entry.product.sku for entry in catalog]

    landing = _campaign_that_lands()
    missing = _campaign_that_fails_both()
    if landing is None or missing is None:
        say('could not find campaign ids with the outcomes this scenario needs')
        return 1

    heading('two campaigns')
    _start_sale(landing, skus[0], 'succeeds — one target is enough')
    _start_sale(missing, skus[1], 'both targets reject — SUCCESS_CASE_NOT_MET')

    deadline = datetime.now(timezone.utc) + timedelta(
        seconds=tuning.PRICE_GOOD_UNTIL_SECONDS,
    )
    heading(
        f'{tuning.EXPIRING_PRICE_SENDS} price updates, '
        f'good for {tuning.PRICE_GOOD_UNTIL_SECONDS} s'
    )
    accepted = 0
    for index in range(tuning.EXPIRING_PRICE_SENDS):
        sku = skus[index % len(skus)]
        match update_price.with_options(good_until=deadline).send(
            sku=sku,
            campaign_id=landing,
        ):
            case Ok(_):
                accepted += 1
            case Err(error):
                say(f'send failed for {sku}: [{error.code}] {error.message}')
    say(f'{accepted} sends accepted; the queue cannot drain them all in time')

    heading('what to watch')
    bullet(f'{WEB_BASE_URL}/?status=EXPIRED  the tail that was never claimed')
    bullet('  EXPIRED is not FAILED — the deadline passed before execution started')
    bullet(f'{WEB_BASE_URL}/workflows  compare the two campaign runs')
    bullet('  the failing one reports WORKFLOW_SUCCESS_CASE_NOT_MET, not a task error')
    bullet('  prewarm_search fails in both and changes nothing — it is optional')
    return 0
