# showcase/acme/scenarios/seed.py
"""Create the demo database, the `acme_*` tables, and the catalog.

Run once before anything else:

    uv run python -m showcase.acme.scenarios seed

The catalog is deterministic: the same SKUs, names, prices, and stock every
time. The last few SKUs are seeded with zero stock — those are the
discontinued lines the shortfall draw routes orders to, which is how an
`INSUFFICIENT_STOCK` failure comes from the database rather than from a script.
"""

from __future__ import annotations

from horsies import Err, Ok

from .. import simulate, store, tuning
from ..domain import Product, StockLevel
from ..settings import DATABASE
from . import WEB_BASE_URL, bullet, heading, say


def _product(index: int) -> Product:
    """One catalog item, derived from its SKU."""
    sku = f'ACME-SKU-{index:04d}'
    return Product(
        sku=sku,
        name=(
            f'{simulate.choice(tuning.PRODUCT_COLOURS, sku, "colour")} '
            f'{simulate.choice(tuning.PRODUCT_LINES, sku, "line")}'
        ),
        category=simulate.choice(tuning.PRODUCT_CATEGORIES, sku, 'category'),
        price_cents=simulate.integer(
            tuning.MIN_PRICE_CENTS, tuning.MAX_PRICE_CENTS, sku, 'price',
        ),
    )


def _catalog() -> tuple[list[Product], list[StockLevel]]:
    """The full catalog and its opening stock levels."""
    products = [_product(index) for index in range(1, tuning.CATALOG_SIZE + 1)]
    first_discontinued = tuning.CATALOG_SIZE - tuning.DISCONTINUED_SKU_COUNT
    stock = [
        StockLevel(
            sku=product.sku,
            on_hand=0 if position >= first_discontinued else tuning.CATALOG_STOCK_PER_SKU,
            reserved=0,
        )
        for position, product in enumerate(products)
    ]
    return products, stock


def run() -> int:
    """Prepare the database. Returns a process exit code."""
    heading('Acme Clothing — seed')
    say(f'database: {DATABASE.database_name}  (resolved from {DATABASE.source})')

    match store.ensure_database():
        case Err(error):
            say(f'cannot reach PostgreSQL: {error.operation} — {error.message}')
            return 1
        case Ok(created):
            say(
                f'created database {DATABASE.database_name}'
                if created
                else f'database {DATABASE.database_name} already exists'
            )

    match store.ensure_schema():
        case Err(error):
            say(f'cannot create the acme tables: {error.message}')
            return 1
        case Ok(_):
            say('acme_* tables ready')

    products, stock = _catalog()
    match store.load_catalog(products, stock):
        case Err(error):
            say(f'cannot load the catalog: {error.message}')
            return 1
        case Ok(loaded):
            say(
                f'loaded {loaded} products, '
                f'{tuning.DISCONTINUED_SKU_COUNT} of them discontinued '
                f'(zero stock), {tuning.CATALOG_STOCK_PER_SKU} units each'
            )

    heading('next')
    bullet('start the four processes from the repository root:')
    bullet('  uv run horsies worker showcase.acme.app:app --processes 12')
    bullet('  uv run horsies scheduler showcase.acme.app:app')
    bullet('  uv run horsies web showcase.acme.app:app --enable-actions')
    bullet('  uv run python -m showcase.acme.scenarios steady')
    bullet(f'then open {WEB_BASE_URL}')
    return 0
