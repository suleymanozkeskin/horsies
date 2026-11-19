//! A `tokio_postgres::Transaction` as the caller's transaction of a rerun.

use horsies_core::{
    CallerColumn, CallerRow, CallerStatement, CallerTransaction, CallerType, StatementValue,
};
use tokio_postgres::types::{FromSql, ToSql};
use tokio_postgres::{Row, Transaction};

/// The caller's `tokio_postgres` transaction, for [`super::rerun_task_in`].
/// Each statement runs with its positional text, each value with its own
/// Rust type (the text casts every parameter), and each column is read as
/// its declared type. Nothing commits: the caller commits or rolls back
/// `transaction`.
pub struct TokioPostgresCaller<'a, 't> {
    transaction: &'a mut Transaction<'t>,
}

impl<'a, 't> TokioPostgresCaller<'a, 't> {
    pub fn new(transaction: &'a mut Transaction<'t>) -> Self {
        Self { transaction }
    }
}

impl CallerTransaction for TokioPostgresCaller<'_, '_> {
    /// The driver's error: the statement failed, it gave no row or more
    /// than one, or a column did not decode as its declared type.
    type Error = tokio_postgres::Error;

    async fn query_one(
        &mut self,
        statement: &CallerStatement,
    ) -> Result<CallerRow, tokio_postgres::Error> {
        let parameters: Vec<&(dyn ToSql + Sync)> =
            statement.parameters().iter().map(sql_value).collect();
        let row = self
            .transaction
            .query_one(statement.sql(), &parameters)
            .await?;
        statement
            .columns()
            .iter()
            .enumerate()
            .map(|(index, column)| column_value(&row, index, column))
            .collect::<Result<Vec<StatementValue>, tokio_postgres::Error>>()
            .map(CallerRow::new)
    }
}

static NULL_BOOL: Option<bool> = None;
static NULL_INT2: Option<i16> = None;
static NULL_INT4: Option<i32> = None;
static NULL_INT8: Option<i64> = None;
static NULL_TEXT: Option<String> = None;
static NULL_BYTEA: Option<Vec<u8>> = None;
static NULL_UUID: Option<uuid::Uuid> = None;
static NULL_TIMESTAMPTZ: Option<chrono::DateTime<chrono::Utc>> = None;

/// The driver value of one parameter. Pure.
fn sql_value(value: &StatementValue) -> &(dyn ToSql + Sync) {
    match value {
        StatementValue::Bool(value) => value,
        StatementValue::Int2(value) => value,
        StatementValue::Int4(value) => value,
        StatementValue::Int8(value) => value,
        StatementValue::Text(value) => value,
        StatementValue::Bytea(value) => value,
        StatementValue::Uuid(value) => value,
        StatementValue::Timestamptz(value) => value,
        StatementValue::Null(value_type) => null_of(*value_type),
    }
}

/// SQL `NULL` of `value_type`. Pure.
fn null_of(value_type: CallerType) -> &'static (dyn ToSql + Sync) {
    match value_type {
        CallerType::Bool => &NULL_BOOL,
        CallerType::Int2 => &NULL_INT2,
        CallerType::Int4 => &NULL_INT4,
        CallerType::Int8 => &NULL_INT8,
        CallerType::Text => &NULL_TEXT,
        CallerType::Bytea => &NULL_BYTEA,
        CallerType::Uuid => &NULL_UUID,
        CallerType::Timestamptz => &NULL_TIMESTAMPTZ,
    }
}

/// The value of column `index` read as the declared type of `column`.
///
/// Errors: the driver's error for a column that the row does not have, or
/// that does not decode as the declared type.
fn column_value(
    row: &Row,
    index: usize,
    column: &CallerColumn,
) -> Result<StatementValue, tokio_postgres::Error> {
    let value_type = column.value_type;
    match value_type {
        CallerType::Bool => read(row, index, value_type, StatementValue::Bool),
        CallerType::Int2 => read(row, index, value_type, StatementValue::Int2),
        CallerType::Int4 => read(row, index, value_type, StatementValue::Int4),
        CallerType::Int8 => read(row, index, value_type, StatementValue::Int8),
        CallerType::Text => read(row, index, value_type, StatementValue::Text),
        CallerType::Bytea => read(row, index, value_type, StatementValue::Bytea),
        CallerType::Uuid => read(row, index, value_type, StatementValue::Uuid),
        CallerType::Timestamptz => read(row, index, value_type, StatementValue::Timestamptz),
    }
}

/// Column `index` as `T`, wrapped by `wrap`; SQL `NULL` is the null of
/// `value_type`.
fn read<T>(
    row: &Row,
    index: usize,
    value_type: CallerType,
    wrap: fn(T) -> StatementValue,
) -> Result<StatementValue, tokio_postgres::Error>
where
    T: for<'a> FromSql<'a>,
{
    let value: Option<T> = row.try_get(index)?;
    Ok(match value {
        Some(value) => wrap(value),
        None => StatementValue::Null(value_type),
    })
}

#[cfg(test)]
mod tests {
    use tokio_postgres::types::{IsNull, Type};

    use super::*;

    /// The text of an encode answer for an assertion message: `IsNull` has
    /// no `Debug`.
    fn encoded_text(encoded: &Result<IsNull, Box<dyn std::error::Error + Sync + Send>>) -> String {
        match encoded {
            Ok(IsNull::No) => "Ok(IsNull::No)".to_owned(),
            Ok(IsNull::Yes) => "Ok(IsNull::Yes)".to_owned(),
            Err(error) => format!("Err({error})"),
        }
    }

    /// Each value encodes as the type that its cast names; a null encodes
    /// as SQL `NULL`.
    #[test]
    fn each_value_encodes_as_its_declared_type() {
        let values = [
            (StatementValue::Bool(true), Type::BOOL),
            (StatementValue::Int2(2), Type::INT2),
            (StatementValue::Int4(4), Type::INT4),
            (StatementValue::Int8(8), Type::INT8),
            (StatementValue::Text("text".to_owned()), Type::TEXT),
            (StatementValue::Bytea(vec![1, 2]), Type::BYTEA),
            (StatementValue::Uuid(uuid::Uuid::nil()), Type::UUID),
            (
                StatementValue::Timestamptz(chrono::Utc::now()),
                Type::TIMESTAMPTZ,
            ),
        ];
        for (value, sql_type) in values {
            let mut written = bytes::BytesMut::new();
            let encoded = sql_value(&value).to_sql_checked(&sql_type, &mut written);
            assert!(
                matches!(encoded, Ok(IsNull::No)),
                "{value:?}: {}",
                encoded_text(&encoded)
            );
            let null = null_of(value.caller_type()).to_sql_checked(&sql_type, &mut written);
            assert!(
                matches!(null, Ok(IsNull::Yes)),
                "{value:?}: {}",
                encoded_text(&null)
            );
        }
    }
}
