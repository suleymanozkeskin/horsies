# showcase/acme/scenarios/maintenance.py
"""Start the back-office workflows once each.

    uv run python -m showcase.acme.scenarios maintenance

The order path is what `steady` exercises. These are the rest of the business:
a routine price sync, a winback campaign, a stock transfer between sites, a
seasonal markdown, and a fraud review. Each is a distinct definition with a
distinct shape, and running them once fills the workflow list with something
other than a hundred identical order runs.

`fraud_review` is worth opening afterwards. Like `returns_review` it is
declared `OnError.PAUSE`, but for a different reason: the pause guards an
irreversible refund rather than a judgement about a returned item.
"""

from __future__ import annotations

from horsies import Err, Ok

from .. import store, tuning
from ..settings import DATABASE
from ..workflows.customer_winback import build_customer_winback
from ..workflows.fraud_review import build_fraud_review
from ..workflows.price_sync import build_price_sync
from ..workflows.seasonal_markdown import build_seasonal_markdown
from ..workflows.warehouse_transfer import build_warehouse_transfer
from . import WEB_BASE_URL, bullet, heading, say


def _started(label: str, workflow_id: str) -> None:
    say(f'{label:22s} ->  {WEB_BASE_URL}/workflows?run={workflow_id}')


def run() -> int:
    """Start one run of each back-office workflow. Returns an exit code."""
    heading('Acme Clothing — maintenance')
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

    heading('back-office runs')

    match build_price_sync(campaign_id='SYNC-0001', sku=skus[0]).start():
        case Err(error):
            say(f'price_sync failed to start: [{error.code}]')
        case Ok(handle):
            _started('price_sync', handle.workflow_id)

    match build_customer_winback(
        segment='winback-manual',
        older_than_minutes=tuning.ABANDONED_CART_AGE_MINUTES,
    ).start():
        case Err(error):
            say(f'customer_winback failed to start: [{error.code}]')
        case Ok(handle):
            _started('customer_winback', handle.workflow_id)

    match build_warehouse_transfer(sku=skus[1], quantity=5).start():
        case Err(error):
            say(f'warehouse_transfer failed to start: [{error.code}]')
        case Ok(handle):
            _started('warehouse_transfer', handle.workflow_id)

    match build_seasonal_markdown(
        campaign_id='MARKDOWN-0001',
        skus=skus[: tuning.FLASH_SALE_SKUS],
    ).start():
        case Err(error):
            say(f'seasonal_markdown failed to start: [{error.code}]')
        case Ok(handle):
            _started('seasonal_markdown', handle.workflow_id)

    match store.list_returnable_orders(1):
        case Err(error):
            say(f'cannot look for a disputed order: {error.message}')
            return 1
        case Ok(orders):
            if not orders:
                say('fraud_review skipped: no captured order to dispute yet')
                say('run steady for a few minutes, then run this scenario again')
                return 0
            disputed_order_id = orders[0][0]

    match build_fraud_review(
        order_id=disputed_order_id,
        amount_cents=4_900,
    ).start():
        case Err(error):
            say(f'fraud_review failed to start: [{error.code}]')
        case Ok(handle):
            _started('fraud_review', handle.workflow_id)

    heading('what to watch')
    bullet(f'{WEB_BASE_URL}/workflows  five differently-shaped runs, side by side')
    bullet('  price_sync joins on all — compare it with flash_sale, same tasks')
    bullet('  seasonal_markdown fans out and joins; catalog_import fans out and does not')
    bullet('  fraud_review pauses if reconciliation fails, before any refund moves')
    return 0
