# showcase/hemline/store.py
"""The `hemline_*` tables and the typed helpers tasks use to read and write them.

Hemline's state is real: products, stock, orders, lines, payments, and
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
    Order,
    OrderLine,
    OrderStatus,
    PaymentIntent,
    PaymentKind,
    Product,
    Shipment,
    StockLevel,
)
from .settings import DATABASE

SCHEMA_SQL: Final[str] = """
CREATE TABLE IF NOT EXISTS hemline_products (
    sku          text PRIMARY KEY,
    name         text NOT NULL,
    category     text NOT NULL,
    price_cents  integer NOT NULL
);

CREATE TABLE IF NOT EXISTS hemline_stock (
    sku        text PRIMARY KEY REFERENCES hemline_products (sku) ON DELETE CASCADE,
    on_hand    integer NOT NULL,
    reserved   integer NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS hemline_orders (
    order_id               text PRIMARY KEY,
    customer_id            text NOT NULL,
    status                 text NOT NULL,
    total_cents            integer NOT NULL,
    authorization_attempts integer NOT NULL DEFAULT 0,
    created_at             timestamptz NOT NULL DEFAULT now(),
    updated_at             timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS hemline_order_lines (
    order_id         text NOT NULL REFERENCES hemline_orders (order_id) ON DELETE CASCADE,
    line_no          integer NOT NULL,
    sku              text NOT NULL,
    size_code        text NOT NULL,
    quantity         integer NOT NULL,
    unit_price_cents integer NOT NULL,
    reserved         boolean NOT NULL DEFAULT false,
    PRIMARY KEY (order_id, line_no)
);

CREATE TABLE IF NOT EXISTS hemline_payments (
    payment_id    text PRIMARY KEY,
    order_id      text NOT NULL REFERENCES hemline_orders (order_id) ON DELETE CASCADE,
    kind          text NOT NULL,
    amount_cents  integer NOT NULL,
    psp_reference text NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (order_id, kind)
);

CREATE TABLE IF NOT EXISTS hemline_shipments (
    shipment_id       text PRIMARY KEY,
    order_id          text NOT NULL UNIQUE
                      REFERENCES hemline_orders (order_id) ON DELETE CASCADE,
    courier           text NOT NULL,
    express           boolean NOT NULL,
    attempts          integer NOT NULL DEFAULT 0,
    booking_reference text,
    label_url         text,
    tracking_code     text,
    created_at        timestamptz NOT NULL DEFAULT now()
);

CREATE SEQUENCE IF NOT EXISTS hemline_order_seq;
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
    """Create the `hemline_*` tables and the order sequence when absent."""

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
                INSERT INTO hemline_products (sku, name, category, price_cents)
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
                INSERT INTO hemline_stock (sku, on_hand, reserved, updated_at)
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
                FROM hemline_products AS p
                JOIN hemline_stock AS s USING (sku)
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
            cursor.execute('SELECT count(*) AS total FROM hemline_products')
            row = cursor.fetchone()
            return 0 if row is None else int(row['total'])

    return _run('count_products', work)


def adjust_stock(sku: str, delta: int) -> StoreResult[bool]:
    """Apply a supplier delta to on-hand stock. False when the SKU is unknown."""

    def work(connection: psycopg.Connection[DictRow]) -> bool:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE hemline_stock
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
            cursor.execute("SELECT nextval('hemline_order_seq') AS value")
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
                INSERT INTO hemline_orders
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
                INSERT INTO hemline_order_lines
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
                FROM hemline_orders
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
                FROM hemline_order_lines
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
                UPDATE hemline_orders
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
                FROM hemline_order_lines
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
                UPDATE hemline_stock
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
                UPDATE hemline_order_lines
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
        'SELECT on_hand - reserved AS available FROM hemline_stock WHERE sku = %s',
        (sku,),
    )
    row = cursor.fetchone()
    return None if row is None else int(row['available'])


def release_line(sku: str, quantity: int) -> StoreResult[int | None]:
    """Hand reserved units back. Returns available units, None for unknown SKU."""

    def work(connection: psycopg.Connection[DictRow]) -> int | None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE hemline_stock
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
                UPDATE hemline_orders
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
                FROM hemline_payments
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
                INSERT INTO hemline_payments
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
                INSERT INTO hemline_shipments
                    (shipment_id, order_id, courier, express, attempts)
                VALUES (%(shipment_id)s, %(order_id)s, %(courier)s, %(express)s, 1)
                ON CONFLICT (order_id) DO UPDATE
                    SET attempts = hemline_shipments.attempts + 1
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
                        'UPDATE hemline_shipments SET {column} = %(value)s '
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


def get_shipment(order_id: str) -> StoreResult[Shipment | None]:
    """The order's shipment row, when one exists."""

    def work(connection: psycopg.Connection[DictRow]) -> Shipment | None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT shipment_id, order_id, courier, express, attempts,
                       booking_reference, label_url, tracking_code
                FROM hemline_shipments
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
