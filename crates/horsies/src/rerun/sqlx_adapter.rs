//! A `sqlx::Transaction<'_, Postgres>` as the caller's transaction of a
//! rerun.

use horsies_core::{
    CallerColumn, CallerRow, CallerStatement, CallerTransaction, CallerType, StatementValue,
};
use sqlx::postgres::{PgArguments, PgRow};
use sqlx::query::Query;
use sqlx::{Postgres, Row, Transaction};

/// The caller's `sqlx` transaction, for [`super::rerun_task_in`]. Each
/// statement runs with its positional text, each value bound with its own
/// Rust type (the text casts every parameter), and each column is read as
/// its declared type. Nothing commits: the caller commits or rolls back
/// `transaction`.
pub struct SqlxCaller<'a, 'c> {
    transaction: &'a mut Transaction<'c, Postgres>,
}

impl<'a, 'c> SqlxCaller<'a, 'c> {
    pub fn new(transaction: &'a mut Transaction<'c, Postgres>) -> Self {
        Self { transaction }
    }
}

impl CallerTransaction for SqlxCaller<'_, '_> {
    /// The driver's error: the statement failed, it gave no row, or a
    /// column did not decode as its declared type.
    type Error = sqlx::Error;

    async fn query_one(&mut self, statement: &CallerStatement) -> Result<CallerRow, sqlx::Error> {
        // An unnamed statement: no prepared statement outlives the call, so
        // a PgBouncer transaction pool with `max_prepared_statements = 0`
        // serves it.
        let query = statement
            .parameters()
            .iter()
            .fold(sqlx::query(statement.sql()).persistent(false), bind);
        let row = query.fetch_one(&mut **self.transaction).await?;
        statement
            .columns()
            .iter()
            .enumerate()
            .map(|(index, column)| column_value(&row, index, column))
            .collect::<Result<Vec<StatementValue>, sqlx::Error>>()
            .map(CallerRow::new)
    }
}

/// `query` with `value` bound as the next parameter. Pure.
fn bind<'q>(
    query: Query<'q, Postgres, PgArguments>,
    value: &'q StatementValue,
) -> Query<'q, Postgres, PgArguments> {
    match value {
        StatementValue::Bool(value) => query.bind(*value),
        StatementValue::Int2(value) => query.bind(*value),
        StatementValue::Int4(value) => query.bind(*value),
        StatementValue::Int8(value) => query.bind(*value),
        StatementValue::Text(value) => query.bind(value.as_str()),
        StatementValue::Bytea(value) => query.bind(value.as_slice()),
        StatementValue::Uuid(value) => query.bind(*value),
        StatementValue::Timestamptz(value) => query.bind(*value),
        StatementValue::Null(value_type) => bind_null(query, *value_type),
    }
}

/// `query` with SQL `NULL` of `value_type` bound as the next parameter.
/// Pure.
fn bind_null(
    query: Query<'_, Postgres, PgArguments>,
    value_type: CallerType,
) -> Query<'_, Postgres, PgArguments> {
    match value_type {
        CallerType::Bool => query.bind(None::<bool>),
        CallerType::Int2 => query.bind(None::<i16>),
        CallerType::Int4 => query.bind(None::<i32>),
        CallerType::Int8 => query.bind(None::<i64>),
        CallerType::Text => query.bind(None::<String>),
        CallerType::Bytea => query.bind(None::<Vec<u8>>),
        CallerType::Uuid => query.bind(None::<uuid::Uuid>),
        CallerType::Timestamptz => query.bind(None::<chrono::DateTime<chrono::Utc>>),
    }
}

/// The value of column `index` read as the declared type of `column`.
///
/// Errors: the driver's error for a column that the row does not have, or
/// that does not decode as the declared type.
fn column_value(
    row: &PgRow,
    index: usize,
    column: &CallerColumn,
) -> Result<StatementValue, sqlx::Error> {
    let value_type = column.value_type;
    let null = StatementValue::Null(value_type);
    Ok(match value_type {
        CallerType::Bool => row
            .try_get::<Option<bool>, _>(index)?
            .map_or(null, StatementValue::Bool),
        CallerType::Int2 => row
            .try_get::<Option<i16>, _>(index)?
            .map_or(null, StatementValue::Int2),
        CallerType::Int4 => row
            .try_get::<Option<i32>, _>(index)?
            .map_or(null, StatementValue::Int4),
        CallerType::Int8 => row
            .try_get::<Option<i64>, _>(index)?
            .map_or(null, StatementValue::Int8),
        CallerType::Text => row
            .try_get::<Option<String>, _>(index)?
            .map_or(null, StatementValue::Text),
        CallerType::Bytea => row
            .try_get::<Option<Vec<u8>>, _>(index)?
            .map_or(null, StatementValue::Bytea),
        CallerType::Uuid => row
            .try_get::<Option<uuid::Uuid>, _>(index)?
            .map_or(null, StatementValue::Uuid),
        CallerType::Timestamptz => row
            .try_get::<Option<chrono::DateTime<chrono::Utc>>, _>(index)?
            .map_or(null, StatementValue::Timestamptz),
    })
}
