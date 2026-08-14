//! SQLx store for the `acme_*` tables.

use std::future::Future;

use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{Postgres, Row, Transaction};
use std::pin::Pin;
use thiserror::Error;

use crate::domain::{
    CatalogEntry, ItemCondition, Order, OrderLine, OrderStatus, PaymentIntent, PaymentKind,
    Product, ReturnCase, ReturnStatus, Shipment, StockLevel,
};
use crate::settings::DatabaseSettings;

pub const SCHEMA_SQL: &str = r#"
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
ALTER TABLE acme_order_lines ADD COLUMN IF NOT EXISTS consumed boolean NOT NULL DEFAULT false;
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
    order_id          text NOT NULL UNIQUE REFERENCES acme_orders (order_id) ON DELETE CASCADE,
    courier            text NOT NULL,
    express            boolean NOT NULL,
    attempts           integer NOT NULL DEFAULT 0,
    booking_reference text,
    label_url          text,
    tracking_code     text,
    created_at         timestamptz NOT NULL DEFAULT now()
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
"#;

#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[error("{operation}: {message}")]
pub struct StoreError {
    pub operation: String,
    pub message: String,
}

pub type StoreResult<T> = Result<T, StoreError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReservationOutcome {
    pub reserved: bool,
    pub replayed: bool,
    pub known_sku: bool,
    pub available: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumptionOutcome {
    pub consumed: bool,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShipmentAttempt {
    pub shipment_id: String,
    pub attempt: i32,
    pub booking_reference: Option<String>,
}

fn store_error(operation: &str, error: impl std::fmt::Display) -> StoreError {
    StoreError {
        operation: operation.to_owned(),
        message: error.to_string(),
    }
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

pub async fn ensure_database(settings: &DatabaseSettings) -> StoreResult<bool> {
    if !valid_identifier(&settings.database_name) {
        return Err(store_error(
            "ensure_database",
            format!("invalid database identifier {:?}", settings.database_name),
        ));
    }
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&settings.maintenance_dsn)
        .await
        .map_err(|error| store_error("ensure_database", error))?;
    let exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)",
    )
    .bind(&settings.database_name)
    .fetch_one(&pool)
    .await
    .map_err(|error| store_error("ensure_database", error))?;
    if exists {
        pool.close().await;
        return Ok(false);
    }
    let statement = format!(
        "CREATE DATABASE \"{}\"",
        settings.database_name.replace('"', "\"\"")
    );
    sqlx::query(&statement)
        .execute(&pool)
        .await
        .map_err(|error| store_error("ensure_database", error))?;
    pool.close().await;
    Ok(true)
}

#[derive(Clone)]
pub struct Store {
    pool: PgPool,
}

impl Store {
    pub async fn connect(settings: &DatabaseSettings) -> StoreResult<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(settings.sqlx_url())
            .await
            .map_err(|error| store_error("connect", error))?;
        Ok(Self { pool })
    }

    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn close(&self) {
        self.pool.close().await;
    }

    async fn transaction<T, F>(&self, operation: &str, work: F) -> StoreResult<T>
    where
        F: for<'a> FnOnce(
            &'a mut Transaction<'_, Postgres>,
        )
            -> Pin<Box<dyn Future<Output = Result<T, sqlx::Error>> + Send + 'a>>,
    {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|error| store_error(operation, error))?;
        match work(&mut tx).await {
            Ok(value) => {
                tx.commit()
                    .await
                    .map_err(|error| store_error(operation, error))?;
                Ok(value)
            }
            Err(error) => {
                let _ = tx.rollback().await;
                Err(store_error(operation, error))
            }
        }
    }

    pub async fn ensure_schema(&self) -> StoreResult<()> {
        self.transaction("ensure_schema", |tx| {
            Box::pin(async move {
                for statement in SCHEMA_SQL
                    .split(';')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                {
                    sqlx::query(statement).execute(&mut **tx).await?;
                }
                Ok(())
            })
        })
        .await
    }

    pub async fn load_catalog(
        &self,
        products: &[Product],
        stock: &[StockLevel],
    ) -> StoreResult<usize> {
        let products = products.to_vec();
        let stock = stock.to_vec();
        self.transaction("load_catalog", |tx| Box::pin(async move {
            for product in &products {
                sqlx::query(
                    "INSERT INTO acme_products (sku,name,category,price_cents) VALUES ($1,$2,$3,$4) \
                     ON CONFLICT (sku) DO UPDATE SET name=EXCLUDED.name, category=EXCLUDED.category, price_cents=EXCLUDED.price_cents",
                )
                .bind(&product.sku)
                .bind(&product.name)
                .bind(&product.category)
                .bind(product.price_cents)
                .execute(&mut **tx)
                .await?;
            }
            for level in stock {
                sqlx::query(
                    "INSERT INTO acme_stock (sku,on_hand,reserved,updated_at) VALUES ($1,$2,$3,now()) \
                     ON CONFLICT (sku) DO UPDATE SET on_hand=EXCLUDED.on_hand, reserved=EXCLUDED.reserved, updated_at=now()",
                )
                .bind(&level.sku)
                .bind(level.on_hand)
                .bind(level.reserved)
                .execute(&mut **tx)
                .await?;
            }
            Ok(products.len())
        }))
        .await
    }

    pub async fn list_catalog(&self) -> StoreResult<Vec<CatalogEntry>> {
        self.transaction("list_catalog", |tx| {
            Box::pin(async move {
                let rows = sqlx::query(
                    "SELECT p.sku,p.name,p.category,p.price_cents,s.on_hand,s.reserved \
                 FROM acme_products p JOIN acme_stock s USING (sku) ORDER BY p.sku",
                )
                .fetch_all(&mut **tx)
                .await?;
                rows.into_iter()
                    .map(|row| {
                        Ok(CatalogEntry {
                            product: Product {
                                sku: row.try_get("sku")?,
                                name: row.try_get("name")?,
                                category: row.try_get("category")?,
                                price_cents: row.try_get("price_cents")?,
                            },
                            stock: StockLevel {
                                sku: row.try_get("sku")?,
                                on_hand: row.try_get("on_hand")?,
                                reserved: row.try_get("reserved")?,
                            },
                        })
                    })
                    .collect()
            })
        })
        .await
    }

    pub async fn count_products(&self) -> StoreResult<i64> {
        self.transaction("count_products", |tx| {
            Box::pin(async move {
                sqlx::query_scalar("SELECT count(*) FROM acme_products")
                    .fetch_one(&mut **tx)
                    .await
            })
        })
        .await
    }

    pub async fn adjust_stock(&self, sku: &str, delta: i32) -> StoreResult<bool> {
        let sku = sku.to_owned();
        self.transaction("adjust_stock", |tx| Box::pin(async move {
            Ok(sqlx::query("UPDATE acme_stock SET on_hand=greatest(0,on_hand+$1),updated_at=now() WHERE sku=$2 RETURNING sku")
                .bind(delta).bind(sku).fetch_optional(&mut **tx).await?.is_some())
        })).await
    }

    pub async fn next_order_number(&self) -> StoreResult<i64> {
        self.transaction("next_order_number", |tx| {
            Box::pin(async move {
                sqlx::query_scalar("SELECT nextval('acme_order_seq')")
                    .fetch_one(&mut **tx)
                    .await
            })
        })
        .await
    }

    pub async fn insert_order(&self, order: &Order) -> StoreResult<()> {
        let order = order.clone();
        self.transaction("insert_order", |tx| Box::pin(async move {
            sqlx::query("INSERT INTO acme_orders (order_id,customer_id,status,total_cents,created_at,updated_at) VALUES ($1,$2,$3,$4,$5,$5)")
                .bind(&order.order_id).bind(&order.customer_id).bind(&order.status)
                .bind(order.total_cents).bind(order.created_at).execute(&mut **tx).await?;
            for line in &order.lines {
                sqlx::query("INSERT INTO acme_order_lines (order_id,line_no,sku,size_code,quantity,unit_price_cents) VALUES ($1,$2,$3,$4,$5,$6)")
                    .bind(&order.order_id).bind(line.line_no).bind(&line.sku).bind(&line.size_code)
                    .bind(line.quantity).bind(line.unit_price_cents).execute(&mut **tx).await?;
            }
            Ok(())
        })).await
    }

    pub async fn get_order(&self, order_id: &str) -> StoreResult<Option<Order>> {
        let order_id = order_id.to_owned();
        self.transaction("get_order", |tx| Box::pin(async move {
            let Some(header) = sqlx::query("SELECT order_id,customer_id,status,total_cents,created_at FROM acme_orders WHERE order_id=$1")
                .bind(&order_id).fetch_optional(&mut **tx).await? else { return Ok(None) };
            let lines = sqlx::query("SELECT line_no,sku,size_code,quantity,unit_price_cents FROM acme_order_lines WHERE order_id=$1 ORDER BY line_no")
                .bind(&order_id).fetch_all(&mut **tx).await?.into_iter().map(|row| Ok(OrderLine {
                    line_no: row.try_get("line_no")?, sku: row.try_get("sku")?, size_code: row.try_get("size_code")?,
                    quantity: row.try_get("quantity")?, unit_price_cents: row.try_get("unit_price_cents")?,
                })).collect::<Result<Vec<_>, sqlx::Error>>()?;
            Ok(Some(Order { order_id: header.try_get("order_id")?, customer_id: header.try_get("customer_id")?,
                status: header.try_get("status")?, total_cents: header.try_get("total_cents")?, lines,
                created_at: header.try_get("created_at")? }))
        })).await
    }

    pub async fn set_order_status(
        &self,
        order_id: &str,
        status: &OrderStatus,
    ) -> StoreResult<bool> {
        let order_id = order_id.to_owned();
        let status = status.clone();
        self.transaction("set_order_status", |tx| Box::pin(async move {
            Ok(sqlx::query("UPDATE acme_orders SET status=$1,updated_at=now() WHERE order_id=$2 RETURNING order_id")
                .bind(status).bind(order_id).fetch_optional(&mut **tx).await?.is_some())
        })).await
    }

    pub async fn reserve_line(
        &self,
        order_id: &str,
        line_no: i32,
        sku: &str,
        quantity: i32,
    ) -> StoreResult<ReservationOutcome> {
        let order_id = order_id.to_owned();
        let sku = sku.to_owned();
        self.transaction("reserve_line", |tx| Box::pin(async move {
            let line = sqlx::query("SELECT reserved FROM acme_order_lines WHERE order_id=$1 AND line_no=$2 FOR UPDATE")
                .bind(&order_id).bind(line_no).fetch_optional(&mut **tx).await?;
            let Some(line) = line else { return Ok(ReservationOutcome { reserved: false, replayed: false, known_sku: false, available: 0 }) };
            if line.try_get::<bool, _>("reserved")? {
                let available = sqlx::query_scalar::<_, i32>(
                    "SELECT on_hand-reserved FROM acme_stock WHERE sku=$1",
                )
                .bind(&sku)
                .fetch_optional(&mut **tx)
                .await?
                .unwrap_or_default();
                return Ok(ReservationOutcome {
                    reserved: true,
                    replayed: true,
                    known_sku: true,
                    available,
                });
            }
            let available: Option<i32> = sqlx::query_scalar("SELECT on_hand-reserved FROM acme_stock WHERE sku=$1").bind(&sku).fetch_optional(&mut **tx).await?;
            let Some(available) = available else { return Ok(ReservationOutcome { reserved: false, replayed: false, known_sku: false, available: 0 }) };
            if available < quantity { return Ok(ReservationOutcome { reserved: false, replayed: false, known_sku: true, available }) }
            let after: i32 = sqlx::query_scalar("UPDATE acme_stock SET reserved=reserved+$1,updated_at=now() WHERE sku=$2 AND on_hand-reserved >= $1 RETURNING on_hand-reserved")
                .bind(quantity).bind(&sku).fetch_one(&mut **tx).await?;
            sqlx::query("UPDATE acme_order_lines SET reserved=true WHERE order_id=$1 AND line_no=$2").bind(&order_id).bind(line_no).execute(&mut **tx).await?;
            Ok(ReservationOutcome { reserved: true, replayed: false, known_sku: true, available: after })
        })).await
    }

    pub async fn consume_line(
        &self,
        order_id: &str,
        line_no: i32,
        sku: &str,
        quantity: i32,
    ) -> StoreResult<ConsumptionOutcome> {
        let order_id = order_id.to_owned();
        let sku = sku.to_owned();
        self.transaction("consume_line", |tx| Box::pin(async move {
            let Some(row) = sqlx::query("SELECT consumed FROM acme_order_lines WHERE order_id=$1 AND line_no=$2 FOR UPDATE")
                .bind(&order_id).bind(line_no).fetch_optional(&mut **tx).await? else { return Ok(ConsumptionOutcome { consumed: false, replayed: false }) };
            if row.try_get::<bool, _>("consumed")? { return Ok(ConsumptionOutcome { consumed: true, replayed: true }) }
            sqlx::query("UPDATE acme_stock SET on_hand=greatest(0,on_hand-$1),reserved=greatest(0,reserved-$1),updated_at=now() WHERE sku=$2")
                .bind(quantity).bind(sku).execute(&mut **tx).await?;
            sqlx::query("UPDATE acme_order_lines SET consumed=true WHERE order_id=$1 AND line_no=$2").bind(&order_id).bind(line_no).execute(&mut **tx).await?;
            Ok(ConsumptionOutcome { consumed: true, replayed: false })
        })).await
    }

    pub async fn nightly_stocktake(
        &self,
        target_units: i32,
        ceiling_units: i32,
    ) -> StoreResult<(i64, i64)> {
        self.transaction("nightly_stocktake", |tx| Box::pin(async move {
            let cleared = sqlx::query("UPDATE acme_stock SET reserved=0,updated_at=now() WHERE reserved>0").execute(&mut **tx).await?.rows_affected() as i64;
            let topped = sqlx::query("UPDATE acme_stock SET on_hand=least(greatest(on_hand,$1),$2),updated_at=now() WHERE on_hand<$1 OR on_hand>$2")
                .bind(target_units).bind(ceiling_units).execute(&mut **tx).await?.rows_affected() as i64;
            Ok((topped, cleared))
        })).await
    }

    pub async fn release_line(&self, sku: &str, quantity: i32) -> StoreResult<Option<i32>> {
        let sku = sku.to_owned();
        self.transaction("release_line", |tx| Box::pin(async move {
            sqlx::query_scalar("UPDATE acme_stock SET reserved=greatest(0,reserved-$1),updated_at=now() WHERE sku=$2 RETURNING on_hand-reserved")
                .bind(quantity).bind(sku).fetch_optional(&mut **tx).await
        })).await
    }

    pub async fn count_authorization_attempt(&self, order_id: &str) -> StoreResult<Option<i32>> {
        let order_id = order_id.to_owned();
        self.transaction("count_authorization_attempt", |tx| Box::pin(async move {
            sqlx::query_scalar("UPDATE acme_orders SET authorization_attempts=authorization_attempts+1,updated_at=now() WHERE order_id=$1 RETURNING authorization_attempts")
                .bind(order_id).fetch_optional(&mut **tx).await
        })).await
    }

    pub async fn find_payment(
        &self,
        order_id: &str,
        kind: &PaymentKind,
    ) -> StoreResult<Option<PaymentIntent>> {
        let order_id = order_id.to_owned();
        let kind = kind.clone();
        self.transaction("find_payment", |tx| Box::pin(async move {
            let Some(row) = sqlx::query("SELECT payment_id,order_id,kind,amount_cents,psp_reference,created_at FROM acme_payments WHERE order_id=$1 AND kind=$2")
                .bind(order_id).bind(kind).fetch_optional(&mut **tx).await? else { return Ok(None) };
            Ok(Some(PaymentIntent { payment_id: row.try_get("payment_id")?, order_id: row.try_get("order_id")?, kind: row.try_get("kind")?, amount_cents: row.try_get("amount_cents")?, psp_reference: row.try_get("psp_reference")?, created_at: row.try_get("created_at")? }))
        })).await
    }

    pub async fn record_payment(
        &self,
        order_id: &str,
        kind: &PaymentKind,
        amount_cents: i32,
        psp_reference: &str,
    ) -> StoreResult<Option<PaymentIntent>> {
        let payment_token = uuid::Uuid::new_v4().simple().to_string();
        let payment_id = format!("pay_{}", &payment_token[..16]);
        let order_id = order_id.to_owned();
        let kind = kind.clone();
        let psp_reference = psp_reference.to_owned();
        self.transaction("record_payment", |tx| Box::pin(async move {
            let row = sqlx::query("INSERT INTO acme_payments (payment_id,order_id,kind,amount_cents,psp_reference) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (order_id,kind) DO NOTHING RETURNING payment_id,order_id,kind,amount_cents,psp_reference,created_at")
                .bind(payment_id).bind(order_id).bind(kind).bind(amount_cents).bind(psp_reference).fetch_optional(&mut **tx).await?;
            row.map(|row| Ok(PaymentIntent { payment_id: row.try_get("payment_id")?, order_id: row.try_get("order_id")?, kind: row.try_get("kind")?, amount_cents: row.try_get("amount_cents")?, psp_reference: row.try_get("psp_reference")?, created_at: row.try_get("created_at")? })).transpose()
        })).await
    }

    pub async fn count_courier_attempt(
        &self,
        order_id: &str,
        courier: &str,
        express: bool,
    ) -> StoreResult<ShipmentAttempt> {
        let shipment_token = uuid::Uuid::new_v4().simple().to_string();
        let shipment_id = format!("shp_{}", &shipment_token[..16]);
        let order_id = order_id.to_owned();
        let courier = courier.to_owned();
        self.transaction("count_courier_attempt", |tx| Box::pin(async move {
            let row = sqlx::query("INSERT INTO acme_shipments (shipment_id,order_id,courier,express,attempts) VALUES ($1,$2,$3,$4,1) ON CONFLICT (order_id) DO UPDATE SET attempts=acme_shipments.attempts+1 RETURNING shipment_id,attempts,booking_reference")
                .bind(shipment_id).bind(order_id).bind(courier).bind(express).fetch_one(&mut **tx).await?;
            Ok(ShipmentAttempt { shipment_id: row.try_get("shipment_id")?, attempt: row.try_get("attempts")?, booking_reference: row.try_get("booking_reference")? })
        })).await
    }

    async fn set_shipment_field(
        &self,
        operation: &str,
        column: &str,
        order_id: &str,
        value: &str,
    ) -> StoreResult<bool> {
        if !matches!(column, "booking_reference" | "label_url" | "tracking_code") {
            return Err(store_error(operation, "invalid shipment column"));
        }
        let statement = format!(
            "UPDATE acme_shipments SET {column}=$1 WHERE order_id=$2 RETURNING shipment_id"
        );
        let operation = operation.to_owned();
        let order_id = order_id.to_owned();
        let value = value.to_owned();
        self.transaction(&operation, |tx| {
            Box::pin(async move {
                Ok(sqlx::query(&statement)
                    .bind(value)
                    .bind(order_id)
                    .fetch_optional(&mut **tx)
                    .await?
                    .is_some())
            })
        })
        .await
    }

    pub async fn set_booking_reference(&self, order_id: &str, value: &str) -> StoreResult<bool> {
        self.set_shipment_field(
            "set_booking_reference",
            "booking_reference",
            order_id,
            value,
        )
        .await
    }
    pub async fn set_label_url(&self, order_id: &str, value: &str) -> StoreResult<bool> {
        self.set_shipment_field("set_label_url", "label_url", order_id, value)
            .await
    }
    pub async fn set_tracking_code(&self, order_id: &str, value: &str) -> StoreResult<bool> {
        self.set_shipment_field("set_tracking_code", "tracking_code", order_id, value)
            .await
    }

    pub async fn next_return_number(&self) -> StoreResult<i64> {
        self.transaction("next_return_number", |tx| {
            Box::pin(async move {
                sqlx::query_scalar("SELECT nextval('acme_return_seq')")
                    .fetch_one(&mut **tx)
                    .await
            })
        })
        .await
    }

    pub async fn open_return(&self, case: &ReturnCase) -> StoreResult<()> {
        let case = case.clone();
        self.transaction("open_return", |tx| Box::pin(async move {
            sqlx::query("INSERT INTO acme_returns (return_id,order_id,sku,quantity,status,condition,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (return_id) DO NOTHING")
                .bind(&case.return_id).bind(&case.order_id).bind(&case.sku).bind(case.quantity).bind(&case.status).bind(case.condition.as_deref()).bind(case.created_at).execute(&mut **tx).await?;
            Ok(())
        })).await
    }

    pub async fn get_return(&self, return_id: &str) -> StoreResult<Option<ReturnCase>> {
        let return_id = return_id.to_owned();
        self.transaction("get_return", |tx| Box::pin(async move {
            let Some(row) = sqlx::query("SELECT return_id,order_id,sku,quantity,status,condition,created_at FROM acme_returns WHERE return_id=$1").bind(return_id).fetch_optional(&mut **tx).await? else { return Ok(None) };
            Ok(Some(ReturnCase { return_id: row.try_get("return_id")?, order_id: row.try_get("order_id")?, sku: row.try_get("sku")?, quantity: row.try_get("quantity")?, status: row.try_get("status")?, condition: row.try_get("condition")?, created_at: row.try_get("created_at")? }))
        })).await
    }

    pub async fn record_inspection(
        &self,
        return_id: &str,
        condition: &ItemCondition,
    ) -> StoreResult<bool> {
        let return_id = return_id.to_owned();
        let condition = condition.clone();
        self.transaction("record_inspection", |tx| Box::pin(async move {
            Ok(sqlx::query("UPDATE acme_returns SET condition=$1,status='inspected' WHERE return_id=$2 RETURNING return_id").bind(condition).bind(return_id).fetch_optional(&mut **tx).await?.is_some())
        })).await
    }

    pub async fn close_return(&self, return_id: &str, status: &ReturnStatus) -> StoreResult<bool> {
        let return_id = return_id.to_owned();
        let status = status.clone();
        self.transaction("close_return", |tx| {
            Box::pin(async move {
                Ok(sqlx::query(
                    "UPDATE acme_returns SET status=$1 WHERE return_id=$2 RETURNING return_id",
                )
                .bind(status)
                .bind(return_id)
                .fetch_optional(&mut **tx)
                .await?
                .is_some())
            })
        })
        .await
    }

    pub async fn list_returnable_orders(
        &self,
        limit: i64,
    ) -> StoreResult<Vec<(String, String, i32)>> {
        self.transaction("list_returnable_orders", |tx| Box::pin(async move {
            let rows = sqlx::query("SELECT o.order_id,ol.sku,ol.quantity FROM acme_orders o JOIN acme_order_lines ol USING (order_id) WHERE o.status='captured' ORDER BY o.created_at DESC LIMIT $1")
                .bind(limit).fetch_all(&mut **tx).await?;
            rows.into_iter().map(|row| Ok((row.try_get("order_id")?, row.try_get("sku")?, row.try_get("quantity")?))).collect()
        })).await
    }

    pub async fn sales_totals(&self) -> StoreResult<(i64, i64, i64)> {
        self.transaction("sales_totals", |tx| {
            Box::pin(async move {
                let (orders, gross): (i64, i64) =
                    sqlx::query_as("SELECT count(*),coalesce(sum(total_cents),0) FROM acme_orders")
                        .fetch_one(&mut **tx)
                        .await?;
                let captured: i64 = sqlx::query_scalar(
                    "SELECT coalesce(sum(amount_cents),0) FROM acme_payments WHERE kind='capture'",
                )
                .fetch_one(&mut **tx)
                .await?;
                Ok((orders, gross, captured))
            })
        })
        .await
    }

    pub async fn abandoned_orders(
        &self,
        older_than_minutes: i32,
    ) -> StoreResult<(i64, Option<String>)> {
        self.transaction("abandoned_orders", |tx| Box::pin(async move {
            let row = sqlx::query("SELECT count(*) AS stranded,min(order_id) AS oldest FROM acme_orders WHERE status NOT IN ('captured','shipped') AND created_at < now() - make_interval(mins => $1)").bind(older_than_minutes).fetch_one(&mut **tx).await?;
            Ok((row.try_get("stranded")?, row.try_get("oldest")?))
        })).await
    }

    pub async fn payment_reconciliation(&self) -> StoreResult<(i64, i64, i64)> {
        self.transaction("payment_reconciliation", |tx| Box::pin(async move {
            let row = sqlx::query("SELECT count(*) FILTER (WHERE kind='authorization') AS authorizations,count(*) FILTER (WHERE kind='capture') AS captures FROM acme_payments").fetch_one(&mut **tx).await?;
            let unmatched: i64 = sqlx::query_scalar("SELECT count(*) FROM acme_payments a WHERE a.kind='authorization' AND NOT EXISTS (SELECT 1 FROM acme_payments c WHERE c.order_id=a.order_id AND c.kind='capture')").fetch_one(&mut **tx).await?;
            Ok((row.try_get("authorizations")?, row.try_get("captures")?, unmatched))
        })).await
    }

    pub async fn get_shipment(&self, order_id: &str) -> StoreResult<Option<Shipment>> {
        let order_id = order_id.to_owned();
        self.transaction("get_shipment", |tx| Box::pin(async move {
            let Some(row) = sqlx::query("SELECT shipment_id,order_id,courier,express,attempts,booking_reference,label_url,tracking_code FROM acme_shipments WHERE order_id=$1").bind(order_id).fetch_optional(&mut **tx).await? else { return Ok(None) };
            Ok(Some(Shipment { shipment_id: row.try_get("shipment_id")?, order_id: row.try_get("order_id")?, courier: row.try_get("courier")?, express: row.try_get("express")?, attempts: row.try_get("attempts")?, booking_reference: row.try_get("booking_reference")?, label_url: row.try_get("label_url")?, tracking_code: row.try_get("tracking_code")? }))
        })).await
    }
}

pub async fn ensure_schema(settings: &DatabaseSettings) -> StoreResult<()> {
    let store = Store::connect(settings).await?;
    let result = store.ensure_schema().await;
    store.close().await;
    result
}
