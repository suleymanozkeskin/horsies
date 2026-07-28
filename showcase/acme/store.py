# showcase/acme/store.py
"""The `acme_*` tables and the typed helpers tasks use to read and write them.

Acme Clothing's state is real: products, stock, orders, lines, payments, and
shipments are rows, and every task in the showcase reads or writes them. That
is what makes the failures honest — an out-of-stock line fails because the
row says so, and a second capture is refused by a unique constraint, not by a
scripted branch.

Every helper returns `StoreResult`. Database exceptions are contained here, at
the one call that performs I/O, and converted into a typed `StoreError`;
nothing above this module handles a psycopg exception.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Final

import psycopg
from psycopg import sql
from psycopg.rows import DictRow, dict_row

from horsies import Err, Ok, Result

from .domain import (
    CatalogEntry,
    ItemCondition,
    Order,
    OrderLine,
    OrderStatus,
    PaymentIntent,
    PaymentKind,
    Product,
    ReturnCase,
    ReturnStatus,
    Shipment,
    StockLevel,
)
from .settings import DATABASE

SCHEMA_SQL: Final[str] = """
CREATE TABLE IF NOT EXISTS acme_products (
    sku          text PRIMARY KEY,
    name         text NOT NULL,
    category     text NOT NULL,
    price_cents  integer NOT NULL
);

CREATE TABLE IF NOT EXISTS acme_stock (
    sku        text PRIMARY KEY REFERENCES acme_products (sku) ON DELETE CASCADE,
    on_hand    integer NOT NULL,
    reserved   integer NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS acme_orders (
    order_id               text PRIMARY KEY,
    customer_id            text NOT NULL,
    status                 text NOT NULL,
    total_cents            integer NOT NULL,
    authorization_attempts integer NOT NULL DEFAULT 0,
    created_at             timestamptz NOT NULL DEFAULT now(),
    updated_at             timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS acme_order_lines (
    order_id         text NOT NULL REFERENCES acme_orders (order_id) ON DELETE CASCADE,
    line_no          integer NOT NULL,
    sku              text NOT NULL,
    size_code        text NOT NULL,
    quantity         integer NOT NULL,
    unit_price_cents integer NOT NULL,
    reserved         boolean NOT NULL DEFAULT false,
    consumed         boolean NOT NULL DEFAULT false,
    PRIMARY KEY (order_id, line_no)
);

-- Databases seeded before the consumption column existed converge here.
ALTER TABLE acme_order_lines
    ADD COLUMN IF NOT EXISTS consumed boolean NOT NULL DEFAULT false;

CREATE TABLE IF NOT EXISTS acme_payments (
    payment_id    text PRIMARY KEY,
    order_id      text NOT NULL REFERENCES acme_orders (order_id) ON DELETE CASCADE,
    kind          text NOT NULL,
    amount_cents  integer NOT NULL,
    psp_reference text NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (order_id, kind)
);

CREATE TABLE IF NOT EXISTS acme_shipments (
    shipment_id       text PRIMARY KEY,
    order_id          text NOT NULL UNIQUE
                      REFERENCES acme_orders (order_id) ON DELETE CASCADE,
    courier           text NOT NULL,
    express           boolean NOT NULL,
    attempts          integer NOT NULL DEFAULT 0,
    booking_reference text,
    label_url         text,
    tracking_code     text,
    created_at        timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS acme_returns (
    return_id  text PRIMARY KEY,
    order_id   text NOT NULL REFERENCES acme_orders (order_id) ON DELETE CASCADE,
    sku        text NOT NULL,
    quantity   integer NOT NULL,
    status     text NOT NULL,
    condition  text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE SEQUENCE IF NOT EXISTS acme_order_seq;
CREATE SEQUENCE IF NOT EXISTS acme_return_seq;
"""


@dataclass(frozen=True, slots=True)
class StoreError:
    """A database operation failed. Carries the operation for the task's error data."""

    operation: str
    message: str
    exception: BaseException | None = None


type StoreResult[T] = Result[T, StoreError]


@dataclass(frozen=True, slots=True)
class ReservationOutcome:
    """Why a reservation succeeded or failed, with the stock it observed."""

    reserved: bool
    replayed: bool
    known_sku: bool
    available: int


@dataclass(frozen=True, slots=True)
class ConsumptionOutcome:
    """Whether a pack consumed its reservation now, or already had."""

    consumed: bool
    replayed: bool


@dataclass(frozen=True, slots=True)
class ShipmentAttempt:
    """The shipment row after counting one booking attempt against it."""

    shipment_id: str
    attempt: int
    booking_reference: str | None


def _run[T](operation: str, work: Callable[[psycopg.Connection[DictRow]], T]) -> StoreResult[T]:
    """Run `work` in one transaction, converting psycopg failures into StoreError."""
    try:
        with psycopg.Connection[DictRow].connect(
            DATABASE.psycopg_dsn, row_factory=dict_row,
        ) as connection:
            return Ok(work(connection))
    except psycopg.Error as error:
        return Err(
            StoreError(
                operation=operation,
                message=str(error).strip() or type(error).__name__,
                exception=error,
            ),
        )


# --- Schema -------------------------------------------------------------


def ensure_database() -> StoreResult[bool]:
    """Create the demo database when absent. True when this call created it."""
    try:
        with psycopg.connect(DATABASE.maintenance_dsn, autocommit=True) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    'SELECT 1 FROM pg_database WHERE datname = %s',
                    (DATABASE.database_name,),
                )
                if cursor.fetchone() is not None:
                    return Ok(False)
                cursor.execute(
                    sql.SQL('CREATE DATABASE {name}').format(
                        name=sql.Identifier(DATABASE.database_name),
                    ),
                )
                return Ok(True)
    except psycopg.Error as error:
        return Err(
            StoreError(
                operation='ensure_database',
                message=str(error).strip() or type(error).__name__,
                exception=error,
            ),
        )


def ensure_schema() -> StoreResult[None]:
    """Create the `acme_*` tables and the order sequence when absent."""

    def work(connection: psycopg.Connection[DictRow]) -> None:
        connection.execute(SCHEMA_SQL)

    return _run('ensure_schema', work)


# --- Catalog ------------------------------------------------------------


def load_catalog(products: Sequence[Product], stock: Sequence[StockLevel]) -> StoreResult[int]:
    """Upsert the catalog and reset stock to the seeded levels."""

    def work(connection: psycopg.Connection[DictRow]) -> int:
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO acme_products (sku, name, category, price_cents)
                VALUES (%(sku)s, %(name)s, %(category)s, %(price_cents)s)
                ON CONFLICT (sku) DO UPDATE
                    SET name = EXCLUDED.name,
                        category = EXCLUDED.category,
                        price_cents = EXCLUDED.price_cents
                """,
                [product.model_dump() for product in products],
            )
            cursor.executemany(
                """
                INSERT INTO acme_stock (sku, on_hand, reserved, updated_at)
                VALUES (%(sku)s, %(on_hand)s, %(reserved)s, now())
                ON CONFLICT (sku) DO UPDATE
                    SET on_hand = EXCLUDED.on_hand,
                        reserved = EXCLUDED.reserved,
                        updated_at = now()
                """,
                [level.model_dump() for level in stock],
            )
        return len(products)

    return _run('load_catalog', work)


def list_catalog() -> StoreResult[list[CatalogEntry]]:
    """Every product with its current stock level."""

    def work(connection: psycopg.Connection[DictRow]) -> list[CatalogEntry]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT p.sku, p.name, p.category, p.price_cents,
                       s.on_hand, s.reserved
                FROM acme_products AS p
                JOIN acme_stock AS s USING (sku)
                ORDER BY p.sku
                """,
            )
            return [
                CatalogEntry(
                    product=Product(
                        sku=row['sku'],
                        name=row['name'],
                        category=row['category'],
                        price_cents=row['price_cents'],
                    ),
                    stock=StockLevel(
                        sku=row['sku'],
                        on_hand=row['on_hand'],
                        reserved=row['reserved'],
                    ),
                )
                for row in cursor.fetchall()
            ]

    return _run('list_catalog', work)


def count_products() -> StoreResult[int]:
    """How many SKUs the catalog holds."""

    def work(connection: psycopg.Connection[DictRow]) -> int:
        with connection.cursor() as cursor:
            cursor.execute('SELECT count(*) AS total FROM acme_products')
            row = cursor.fetchone()
            return 0 if row is None else int(row['total'])

    return _run('count_products', work)


def adjust_stock(sku: str, delta: int) -> StoreResult[bool]:
    """Apply a supplier delta to on-hand stock. False when the SKU is unknown."""

    def work(connection: psycopg.Connection[DictRow]) -> bool:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE acme_stock
                SET on_hand = greatest(0, on_hand + %(delta)s), updated_at = now()
                WHERE sku = %(sku)s
                RETURNING sku
                """,
                {'sku': sku, 'delta': delta},
            )
            return cursor.fetchone() is not None

    return _run('adjust_stock', work)


# --- Orders -------------------------------------------------------------


def next_order_number() -> StoreResult[int]:
    """Next value of the order sequence — stable ids without collisions."""

    def work(connection: psycopg.Connection[DictRow]) -> int:
        with connection.cursor() as cursor:
            cursor.execute("SELECT nextval('acme_order_seq') AS value")
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError('nextval returned no row')
            return int(row['value'])

    return _run('next_order_number', work)


def insert_order(order: Order) -> StoreResult[None]:
    """Write the order and its lines in one transaction."""

    def work(connection: psycopg.Connection[DictRow]) -> None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO acme_orders
                    (order_id, customer_id, status, total_cents, created_at, updated_at)
                VALUES (%(order_id)s, %(customer_id)s, %(status)s, %(total_cents)s,
                        %(created_at)s, %(created_at)s)
                """,
                {
                    'order_id': order.order_id,
                    'customer_id': order.customer_id,
                    'status': order.status,
                    'total_cents': order.total_cents,
                    'created_at': order.created_at,
                },
            )
            cursor.executemany(
                """
                INSERT INTO acme_order_lines
                    (order_id, line_no, sku, size_code, quantity, unit_price_cents)
                VALUES (%(order_id)s, %(line_no)s, %(sku)s, %(size_code)s,
                        %(quantity)s, %(unit_price_cents)s)
                """,
                [
                    {'order_id': order.order_id, **line.model_dump()}
                    for line in order.lines
                ],
            )

    return _run('insert_order', work)


def get_order(order_id: str) -> StoreResult[Order | None]:
    """Load an order with its lines. None when the id is unknown."""

    def work(connection: psycopg.Connection[DictRow]) -> Order | None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT order_id, customer_id, status, total_cents, created_at
                FROM acme_orders
                WHERE order_id = %s
                """,
                (order_id,),
            )
            header = cursor.fetchone()
            if header is None:
                return None
            cursor.execute(
                """
                SELECT line_no, sku, size_code, quantity, unit_price_cents
                FROM acme_order_lines
                WHERE order_id = %s
                ORDER BY line_no
                """,
                (order_id,),
            )
            lines = [
                OrderLine(
                    line_no=row['line_no'],
                    sku=row['sku'],
                    size_code=row['size_code'],
                    quantity=row['quantity'],
                    unit_price_cents=row['unit_price_cents'],
                )
                for row in cursor.fetchall()
            ]
        return Order(
            order_id=header['order_id'],
            customer_id=header['customer_id'],
            status=header['status'],
            total_cents=header['total_cents'],
            lines=lines,
            created_at=header['created_at'],
        )

    return _run('get_order', work)


def set_order_status(order_id: str, status: OrderStatus) -> StoreResult[bool]:
    """Move an order to `status`. False when the id is unknown."""

    def work(connection: psycopg.Connection[DictRow]) -> bool:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE acme_orders
                SET status = %(status)s, updated_at = now()
                WHERE order_id = %(order_id)s
                RETURNING order_id
                """,
                {'order_id': order_id, 'status': status},
            )
            return cursor.fetchone() is not None

    return _run('set_order_status', work)


# --- Stock reservation --------------------------------------------------


def reserve_line(
    order_id: str,
    line_no: int,
    sku: str,
    quantity: int,
) -> StoreResult[ReservationOutcome]:
    """Reserve one order line against real stock, idempotently.

    A line already marked reserved returns `replayed=True` without touching
    stock, so a crash-recovery re-run never double-reserves.
    """

    def work(connection: psycopg.Connection[DictRow]) -> ReservationOutcome:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT reserved
                FROM acme_order_lines
                WHERE order_id = %(order_id)s AND line_no = %(line_no)s
                FOR UPDATE
                """,
                {'order_id': order_id, 'line_no': line_no},
            )
            line = cursor.fetchone()
            available = _read_available(cursor, sku)
            if line is not None and line['reserved'] is True:
                return ReservationOutcome(
                    reserved=True,
                    replayed=True,
                    known_sku=available is not None,
                    available=available or 0,
                )
            cursor.execute(
                """
                UPDATE acme_stock
                SET reserved = reserved + %(quantity)s, updated_at = now()
                WHERE sku = %(sku)s AND on_hand - reserved >= %(quantity)s
                RETURNING on_hand - reserved AS available
                """,
                {'sku': sku, 'quantity': quantity},
            )
            updated = cursor.fetchone()
            if updated is None:
                return ReservationOutcome(
                    reserved=False,
                    replayed=False,
                    known_sku=available is not None,
                    available=available or 0,
                )
            cursor.execute(
                """
                UPDATE acme_order_lines
                SET reserved = true
                WHERE order_id = %(order_id)s AND line_no = %(line_no)s
                """,
                {'order_id': order_id, 'line_no': line_no},
            )
            return ReservationOutcome(
                reserved=True,
                replayed=False,
                known_sku=True,
                available=int(updated['available']),
            )

    return _run('reserve_line', work)


def _read_available(cursor: psycopg.Cursor[DictRow], sku: str) -> int | None:
    """Available units for a SKU, or None when the SKU is unknown."""
    cursor.execute(
        'SELECT on_hand - reserved AS available FROM acme_stock WHERE sku = %s',
        (sku,),
    )
    row = cursor.fetchone()
    return None if row is None else int(row['available'])


def consume_line(
    order_id: str,
    line_no: int,
    sku: str,
    quantity: int,
) -> StoreResult[ConsumptionOutcome]:
    """Convert one line's reservation into shipped units, idempotently.

    Packing takes the units off the shelf: both `on_hand` and `reserved`
    drop by the line quantity, so a completed order stops holding stock.
    A line already marked consumed returns `replayed=True` without touching
    stock, so a crash-recovery re-run never double-consumes.
    """

    def work(connection: psycopg.Connection[DictRow]) -> ConsumptionOutcome:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT consumed
                FROM acme_order_lines
                WHERE order_id = %(order_id)s AND line_no = %(line_no)s
                FOR UPDATE
                """,
                {'order_id': order_id, 'line_no': line_no},
            )
            line = cursor.fetchone()
            if line is None:
                return ConsumptionOutcome(consumed=False, replayed=False)
            if line['consumed'] is True:
                return ConsumptionOutcome(consumed=True, replayed=True)
            cursor.execute(
                """
                UPDATE acme_stock
                SET on_hand  = greatest(0, on_hand - %(quantity)s),
                    reserved = greatest(0, reserved - %(quantity)s),
                    updated_at = now()
                WHERE sku = %(sku)s
                """,
                {'sku': sku, 'quantity': quantity},
            )
            cursor.execute(
                """
                UPDATE acme_order_lines
                SET consumed = true
                WHERE order_id = %(order_id)s AND line_no = %(line_no)s
                """,
                {'order_id': order_id, 'line_no': line_no},
            )
            return ConsumptionOutcome(consumed=True, replayed=False)

    return _run('consume_line', work)


def nightly_stocktake(
    target_units: int,
    ceiling_units: int,
) -> StoreResult[tuple[int, int]]:
    """Reset the warehouse to a healthy morning state.

    Clears every standing reservation (orders that failed after reserving
    leak theirs; 04:00 has almost no orders in flight, and an in-flight
    line keeps its own `reserved`/`consumed` flags, so replay stays
    correct), then tops each SKU up to at least `target_units` and caps
    runaway restock accumulation at `ceiling_units`. Returns
    ``(skus_topped_up, reservations_cleared)``.
    """

    def work(connection: psycopg.Connection[DictRow]) -> tuple[int, int]:
        with connection.cursor() as cursor:
            cursor.execute(
                'UPDATE acme_stock SET reserved = 0, updated_at = now() '
                'WHERE reserved > 0',
            )
            cleared = cursor.rowcount
            cursor.execute(
                """
                UPDATE acme_stock
                SET on_hand = least(greatest(on_hand, %(target)s), %(ceiling)s),
                    updated_at = now()
                WHERE on_hand < %(target)s OR on_hand > %(ceiling)s
                """,
                {'target': target_units, 'ceiling': ceiling_units},
            )
            return (cursor.rowcount, cleared)

    return _run('nightly_stocktake', work)


def release_line(sku: str, quantity: int) -> StoreResult[int | None]:
    """Hand reserved units back. Returns available units, None for unknown SKU."""

    def work(connection: psycopg.Connection[DictRow]) -> int | None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE acme_stock
                SET reserved = greatest(0, reserved - %(quantity)s), updated_at = now()
                WHERE sku = %(sku)s
                RETURNING on_hand - reserved AS available
                """,
                {'sku': sku, 'quantity': quantity},
            )
            row = cursor.fetchone()
            return None if row is None else int(row['available'])

    return _run('release_line', work)


# --- Payments -----------------------------------------------------------


def count_authorization_attempt(order_id: str) -> StoreResult[int | None]:
    """Count one authorization attempt. None when the order is unknown."""

    def work(connection: psycopg.Connection[DictRow]) -> int | None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE acme_orders
                SET authorization_attempts = authorization_attempts + 1,
                    updated_at = now()
                WHERE order_id = %s
                RETURNING authorization_attempts
                """,
                (order_id,),
            )
            row = cursor.fetchone()
            return None if row is None else int(row['authorization_attempts'])

    return _run('count_authorization_attempt', work)


def find_payment(order_id: str, kind: PaymentKind) -> StoreResult[PaymentIntent | None]:
    """The order's payment row of this kind, when one exists."""

    def work(connection: psycopg.Connection[DictRow]) -> PaymentIntent | None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT payment_id, order_id, kind, amount_cents, psp_reference, created_at
                FROM acme_payments
                WHERE order_id = %(order_id)s AND kind = %(kind)s
                """,
                {'order_id': order_id, 'kind': kind},
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return PaymentIntent(
                payment_id=row['payment_id'],
                order_id=row['order_id'],
                kind=row['kind'],
                amount_cents=row['amount_cents'],
                psp_reference=row['psp_reference'],
                created_at=row['created_at'],
            )

    return _run('find_payment', work)


def record_payment(
    order_id: str,
    kind: PaymentKind,
    amount_cents: int,
    psp_reference: str,
) -> StoreResult[PaymentIntent | None]:
    """Insert a payment row. None when one of this kind already exists."""

    def work(connection: psycopg.Connection[DictRow]) -> PaymentIntent | None:
        payment = PaymentIntent(
            payment_id=f'pay_{uuid.uuid4().hex[:16]}',
            order_id=order_id,
            kind=kind,
            amount_cents=amount_cents,
            psp_reference=psp_reference,
            created_at=datetime.now(timezone.utc),
        )
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO acme_payments
                    (payment_id, order_id, kind, amount_cents, psp_reference, created_at)
                VALUES (%(payment_id)s, %(order_id)s, %(kind)s, %(amount_cents)s,
                        %(psp_reference)s, %(created_at)s)
                ON CONFLICT (order_id, kind) DO NOTHING
                RETURNING payment_id
                """,
                payment.model_dump(),
            )
            return None if cursor.fetchone() is None else payment

    return _run('record_payment', work)


# --- Shipments ----------------------------------------------------------


def count_courier_attempt(
    order_id: str,
    courier: str,
    express: bool,
) -> StoreResult[ShipmentAttempt]:
    """Create or touch the shipment row and count one booking attempt."""

    def work(connection: psycopg.Connection[DictRow]) -> ShipmentAttempt:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO acme_shipments
                    (shipment_id, order_id, courier, express, attempts)
                VALUES (%(shipment_id)s, %(order_id)s, %(courier)s, %(express)s, 1)
                ON CONFLICT (order_id) DO UPDATE
                    SET attempts = acme_shipments.attempts + 1
                RETURNING shipment_id, attempts, booking_reference
                """,
                {
                    'shipment_id': f'shp_{uuid.uuid4().hex[:16]}',
                    'order_id': order_id,
                    'courier': courier,
                    'express': express,
                },
            )
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError('shipment upsert returned no row')
            return ShipmentAttempt(
                shipment_id=row['shipment_id'],
                attempt=int(row['attempts']),
                booking_reference=row['booking_reference'],
            )

    return _run('count_courier_attempt', work)


def _set_shipment_field(operation: str, column: str) -> Callable[[str, str], StoreResult[bool]]:
    """Build a setter for one nullable shipment column."""

    def setter(order_id: str, value: str) -> StoreResult[bool]:
        def work(connection: psycopg.Connection[DictRow]) -> bool:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql.SQL(
                        'UPDATE acme_shipments SET {column} = %(value)s '
                        'WHERE order_id = %(order_id)s RETURNING shipment_id'
                    ).format(column=sql.Identifier(column)),
                    {'order_id': order_id, 'value': value},
                )
                return cursor.fetchone() is not None

        return _run(operation, work)

    return setter


set_booking_reference = _set_shipment_field('set_booking_reference', 'booking_reference')
set_label_url = _set_shipment_field('set_label_url', 'label_url')
set_tracking_code = _set_shipment_field('set_tracking_code', 'tracking_code')


# --- Returns ------------------------------------------------------------


def next_return_number() -> StoreResult[int]:
    """Next value of the return sequence."""

    def work(connection: psycopg.Connection[DictRow]) -> int:
        with connection.cursor() as cursor:
            cursor.execute("SELECT nextval('acme_return_seq') AS value")
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError('nextval returned no row')
            return int(row['value'])

    return _run('next_return_number', work)


def open_return(case: ReturnCase) -> StoreResult[None]:
    """Book a return into the returns desk."""

    def work(connection: psycopg.Connection[DictRow]) -> None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO acme_returns
                    (return_id, order_id, sku, quantity, status, condition, created_at)
                VALUES (%(return_id)s, %(order_id)s, %(sku)s, %(quantity)s,
                        %(status)s, %(condition)s, %(created_at)s)
                ON CONFLICT (return_id) DO NOTHING
                """,
                case.model_dump(),
            )

    return _run('open_return', work)


def get_return(return_id: str) -> StoreResult[ReturnCase | None]:
    """Load a return case."""

    def work(connection: psycopg.Connection[DictRow]) -> ReturnCase | None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT return_id, order_id, sku, quantity, status, condition, created_at
                FROM acme_returns
                WHERE return_id = %s
                """,
                (return_id,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return ReturnCase(
                return_id=row['return_id'],
                order_id=row['order_id'],
                sku=row['sku'],
                quantity=row['quantity'],
                status=row['status'],
                condition=row['condition'],
                created_at=row['created_at'],
            )

    return _run('get_return', work)


def record_inspection(
    return_id: str,
    condition: ItemCondition,
) -> StoreResult[bool]:
    """Store what the inspector found."""

    def work(connection: psycopg.Connection[DictRow]) -> bool:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE acme_returns
                SET condition = %(condition)s, status = 'inspected'
                WHERE return_id = %(return_id)s
                RETURNING return_id
                """,
                {'return_id': return_id, 'condition': condition},
            )
            return cursor.fetchone() is not None

    return _run('record_inspection', work)


def close_return(return_id: str, status: ReturnStatus) -> StoreResult[bool]:
    """Move a return to its terminal status."""

    def work(connection: psycopg.Connection[DictRow]) -> bool:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE acme_returns
                SET status = %(status)s
                WHERE return_id = %(return_id)s
                RETURNING return_id
                """,
                {'return_id': return_id, 'status': status},
            )
            return cursor.fetchone() is not None

    return _run('close_return', work)


def list_returnable_orders(limit: int) -> StoreResult[list[tuple[str, str, int]]]:
    """Captured orders with a line, as (order_id, sku, quantity)."""

    def work(connection: psycopg.Connection[DictRow]) -> list[tuple[str, str, int]]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT o.order_id, ol.sku, ol.quantity
                FROM acme_orders AS o
                JOIN acme_order_lines AS ol USING (order_id)
                WHERE o.status = 'captured'
                ORDER BY o.created_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            return [
                (row['order_id'], row['sku'], int(row['quantity']))
                for row in cursor.fetchall()
            ]

    return _run('list_returnable_orders', work)


# --- Analytics ----------------------------------------------------------


def sales_totals() -> StoreResult[tuple[int, int, int]]:
    """(orders, gross cents, captured cents) across the whole demo database."""

    def work(connection: psycopg.Connection[DictRow]) -> tuple[int, int, int]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT count(*) AS orders, coalesce(sum(total_cents), 0) AS gross
                FROM acme_orders
                """,
            )
            header = cursor.fetchone()
            cursor.execute(
                """
                SELECT coalesce(sum(amount_cents), 0) AS captured
                FROM acme_payments
                WHERE kind = 'capture'
                """,
            )
            captured = cursor.fetchone()
        orders = 0 if header is None else int(header['orders'])
        gross = 0 if header is None else int(header['gross'])
        settled = 0 if captured is None else int(captured['captured'])
        return orders, gross, settled

    return _run('sales_totals', work)


def abandoned_orders(older_than_minutes: int) -> StoreResult[tuple[int, str | None]]:
    """(count, oldest order id) for orders that never reached a capture."""

    def work(connection: psycopg.Connection[DictRow]) -> tuple[int, str | None]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT count(*) AS stranded, min(order_id) AS oldest
                FROM acme_orders
                WHERE status NOT IN ('captured', 'shipped')
                  AND created_at < now() - make_interval(mins => %s)
                """,
                (older_than_minutes,),
            )
            row = cursor.fetchone()
            if row is None:
                return 0, None
            return int(row['stranded']), row['oldest']

    return _run('abandoned_orders', work)


def payment_reconciliation() -> StoreResult[tuple[int, int, int]]:
    """(authorizations, captures, authorizations without a capture)."""

    def work(connection: psycopg.Connection[DictRow]) -> tuple[int, int, int]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    count(*) FILTER (WHERE kind = 'authorization') AS authorizations,
                    count(*) FILTER (WHERE kind = 'capture') AS captures
                FROM acme_payments
                """,
            )
            totals = cursor.fetchone()
            cursor.execute(
                """
                SELECT count(*) AS unmatched
                FROM acme_payments AS a
                WHERE a.kind = 'authorization'
                  AND NOT EXISTS (
                      SELECT 1 FROM acme_payments AS c
                      WHERE c.order_id = a.order_id AND c.kind = 'capture'
                  )
                """,
            )
            gap = cursor.fetchone()
        authorizations = 0 if totals is None else int(totals['authorizations'])
        captures = 0 if totals is None else int(totals['captures'])
        unmatched = 0 if gap is None else int(gap['unmatched'])
        return authorizations, captures, unmatched

    return _run('payment_reconciliation', work)


def get_shipment(order_id: str) -> StoreResult[Shipment | None]:
    """The order's shipment row, when one exists."""

    def work(connection: psycopg.Connection[DictRow]) -> Shipment | None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT shipment_id, order_id, courier, express, attempts,
                       booking_reference, label_url, tracking_code
                FROM acme_shipments
                WHERE order_id = %s
                """,
                (order_id,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return Shipment(
                shipment_id=row['shipment_id'],
                order_id=row['order_id'],
                courier=row['courier'],
                express=row['express'],
                attempts=row['attempts'],
                booking_reference=row['booking_reference'],
                label_url=row['label_url'],
                tracking_code=row['tracking_code'],
            )

    return _run('get_shipment', work)
