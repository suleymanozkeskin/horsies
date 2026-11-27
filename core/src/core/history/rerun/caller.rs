//! One core statement in a transaction that the caller owns (Python
//! `rerun_task_in_async(connection, ...)`: the core builds each statement, and the
//! caller's connection runs it, so all of it commits or rolls back with the
//! caller's own writes).
//!
//! - [`CallerStatement`]: a frozen core statement, its parameter values and
//!   its result columns. Its SQL text is positional (`$1`, `$2`, ...) with a
//!   cast on each parameter, and every value and column has a built-in type
//!   ([`CallerType`]), so the caller's driver infers nothing and needs no
//!   type extension.
//! - [`CallerTransaction`]: the one method a transaction needs, to run one
//!   such statement and give its one row. The Rust client implements it for
//!   `tokio_postgres::Transaction` and `sqlx::Transaction<'_, Postgres>`.
//!   The core's own [`Transaction`] implements it here, so the core's own
//!   path needs no adapter.
//!
//! Nothing here commits: the caller commits or rolls back.

use std::future::Future;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::broker::core_rows::{one_row, RowDecodeError};
use crate::db::{
    ParameterBinder, ParameterType, Row, StatementBound, StatementFailure, Transaction,
    TypedStatement,
};

/// The types a caller statement binds and reads: built-in PostgreSQL types
/// that every driver encodes and decodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallerType {
    Bool,
    Int2,
    Int4,
    Int8,
    Text,
    Bytea,
    Uuid,
    Timestamptz,
}

impl CallerType {
    /// The SQL name, as the casts of the statement text spell it.
    pub fn sql_name(self) -> &'static str {
        self.parameter_type().sql_name()
    }

    fn parameter_type(self) -> ParameterType {
        match self {
            Self::Bool => ParameterType::Bool,
            Self::Int2 => ParameterType::Int2,
            Self::Int4 => ParameterType::Int4,
            Self::Int8 => ParameterType::Int8,
            Self::Text => ParameterType::Text,
            Self::Bytea => ParameterType::Bytea,
            Self::Uuid => ParameterType::Uuid,
            Self::Timestamptz => ParameterType::Timestamptz,
        }
    }

    /// The caller type of a parameter type, or `None` for a type that a
    /// caller statement does not use (floats, JSON, arrays).
    fn of_parameter(parameter: ParameterType) -> Option<Self> {
        match parameter {
            ParameterType::Bool => Some(Self::Bool),
            ParameterType::Int2 => Some(Self::Int2),
            ParameterType::Int4 => Some(Self::Int4),
            ParameterType::Int8 => Some(Self::Int8),
            ParameterType::Text => Some(Self::Text),
            ParameterType::Bytea => Some(Self::Bytea),
            ParameterType::Uuid => Some(Self::Uuid),
            ParameterType::Timestamptz => Some(Self::Timestamptz),
            ParameterType::Float4
            | ParameterType::Float8
            | ParameterType::Json
            | ParameterType::Jsonb
            | ParameterType::BoolArray
            | ParameterType::Int2Array
            | ParameterType::Int4Array
            | ParameterType::Int8Array
            | ParameterType::Float8Array
            | ParameterType::TextArray
            | ParameterType::ByteaArray
            | ParameterType::UuidArray
            | ParameterType::TimestamptzArray
            | ParameterType::JsonbArray => None,
        }
    }
}

/// One value of a parameter or of a result column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatementValue {
    Bool(bool),
    Int2(i16),
    Int4(i32),
    Int8(i64),
    Text(String),
    Bytea(Vec<u8>),
    Uuid(Uuid),
    Timestamptz(DateTime<Utc>),
    /// SQL `NULL` of this type.
    Null(CallerType),
}

impl StatementValue {
    /// The type of the value.
    pub fn caller_type(&self) -> CallerType {
        match self {
            Self::Bool(_) => CallerType::Bool,
            Self::Int2(_) => CallerType::Int2,
            Self::Int4(_) => CallerType::Int4,
            Self::Int8(_) => CallerType::Int8,
            Self::Text(_) => CallerType::Text,
            Self::Bytea(_) => CallerType::Bytea,
            Self::Uuid(_) => CallerType::Uuid,
            Self::Timestamptz(_) => CallerType::Timestamptz,
            Self::Null(value_type) => *value_type,
        }
    }
}

/// One result column of a caller statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CallerColumn {
    pub name: &'static str,
    pub value_type: CallerType,
}

/// Why the core cannot build a caller statement: a defect of the core code
/// that builds it. Each names the statement.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CallerStatementRefused {
    #[error("statement `{statement}` declares {declared} parameters; {given} values were given")]
    CountMismatch {
        statement: &'static str,
        declared: usize,
        given: usize,
    },
    #[error("statement `{statement}` parameter ${position} is {declared}; the value is {given:?}")]
    TypeMismatch {
        statement: &'static str,
        position: usize,
        declared: &'static str,
        given: CallerType,
    },
    #[error(
        "statement `{statement}` parameter ${position} is {declared}, which a caller statement \
         does not use"
    )]
    UnsupportedType {
        statement: &'static str,
        position: usize,
        declared: &'static str,
    },
    #[error("statement `{statement}` names ${position}, which it does not declare")]
    UndeclaredParameter {
        statement: &'static str,
        position: usize,
    },
}

/// One core statement for a transaction that the caller owns: the frozen
/// statement, one value per parameter, and the result columns in order.
/// Frozen once built.
#[derive(Debug, Clone)]
pub struct CallerStatement {
    name: &'static str,
    statement: &'static TypedStatement,
    sql: String,
    named_sql: String,
    parameters: Vec<StatementValue>,
    columns: &'static [CallerColumn],
}

/// Two caller statements are equal when their name, text, values and
/// columns are: the frozen statement is named by its name and text.
impl PartialEq for CallerStatement {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.sql == other.sql
            && self.parameters == other.parameters
            && self.columns == other.columns
    }
}

impl Eq for CallerStatement {}

impl CallerStatement {
    /// `statement` with `parameters`, read as `columns`.
    ///
    /// Errors: [`CallerStatementRefused`] when the values do not fit the
    /// declared parameters (count, then type, in order), when a declared
    /// type is not a [`CallerType`], or when the text names a parameter it
    /// does not declare.
    pub fn new(
        statement: &'static TypedStatement,
        parameters: Vec<StatementValue>,
        columns: &'static [CallerColumn],
    ) -> Result<Self, CallerStatementRefused> {
        let declared = statement.parameter_types();
        if declared.len() != parameters.len() {
            return Err(CallerStatementRefused::CountMismatch {
                statement: statement.name(),
                declared: declared.len(),
                given: parameters.len(),
            });
        }
        for (index, (declared, value)) in declared.iter().zip(&parameters).enumerate() {
            let position = index + 1;
            match CallerType::of_parameter(*declared) {
                None => {
                    return Err(CallerStatementRefused::UnsupportedType {
                        statement: statement.name(),
                        position,
                        declared: declared.sql_name(),
                    })
                }
                Some(expected) if expected != value.caller_type() => {
                    return Err(CallerStatementRefused::TypeMismatch {
                        statement: statement.name(),
                        position,
                        declared: declared.sql_name(),
                        given: value.caller_type(),
                    })
                }
                Some(_) => {}
            }
        }
        let sql = positional_sql(statement)?;
        let named_sql = named_bind_sql(statement)?;
        Ok(Self {
            name: statement.name(),
            statement,
            sql,
            named_sql,
            parameters,
            columns,
        })
    }

    /// The statement's name, for errors and logs.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// The positional SQL text, with a cast on each parameter.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// The SQL text for a driver with named binds (SQLAlchemy `text()`):
    /// each `$n` is `CAST(:p<n> AS <type>)` ([`named_bind_sql`]); the value of
    /// `:p<n>` is `parameters()[n - 1]` ([`named_bind`]).
    pub fn named_sql(&self) -> &str {
        &self.named_sql
    }

    /// The frozen statement, for the statement log of the core's own path.
    pub(crate) fn typed_statement(&self) -> &'static TypedStatement {
        self.statement
    }

    /// One value per parameter: `$1` is the first.
    pub fn parameters(&self) -> &[StatementValue] {
        &self.parameters
    }

    /// The result columns, in the order of the row.
    pub fn columns(&self) -> &'static [CallerColumn] {
        self.columns
    }
}

/// The text of `statement` with a cast on each `$n` that has none, outside
/// quoted literals: `$1` becomes `$1::uuid`. Pure.
///
/// Errors: [`CallerStatementRefused::UndeclaredParameter`] for a `$n` past
/// the declared parameters, or `$0`.
pub fn positional_sql(statement: &TypedStatement) -> Result<String, CallerStatementRefused> {
    render_parameters(
        statement,
        |position, parameter, cast_follows| match cast_follows {
            true => format!("${position}"),
            false => format!("${position}::{}", parameter.sql_name()),
        },
    )
}

/// The name of the bind of parameter `$position` in [`named_bind_sql`]:
/// `p1` for `$1`.
pub fn named_bind(position: usize) -> String {
    format!("p{position}")
}

/// The text of `statement` for a driver with named binds (SQLAlchemy
/// `text()`): each `$n` outside quoted literals becomes
/// `CAST(:p<n> AS <type>)`, so the driver infers no type. A cast that the
/// text writes after `$n` stays and applies to the cast value. Pure.
///
/// Errors: as [`positional_sql`].
pub fn named_bind_sql(statement: &TypedStatement) -> Result<String, CallerStatementRefused> {
    render_parameters(statement, |position, parameter, _cast_follows| {
        format!(
            "CAST(:{} AS {})",
            named_bind(position),
            parameter.sql_name()
        )
    })
}

/// The text of `statement` with each `$n` outside quoted literals replaced
/// by `placeholder(n, declared type, whether "::" follows)`. Pure.
///
/// Errors: [`CallerStatementRefused::UndeclaredParameter`] for a `$n` past
/// the declared parameters, or `$0`.
fn render_parameters(
    statement: &TypedStatement,
    placeholder: impl Fn(usize, ParameterType, bool) -> String,
) -> Result<String, CallerStatementRefused> {
    let sql = statement.sql();
    let declared = statement.parameter_types();
    let mut text = String::with_capacity(sql.len() + declared.len() * 24);
    let mut in_literal = false;
    let mut chars = sql.char_indices().peekable();
    while let Some((index, character)) = chars.next() {
        match (in_literal, character) {
            (true, '\'') | (false, '\'') => {
                in_literal = !in_literal;
                text.push(character);
            }
            (false, '$') => {
                let start = index + 1;
                let mut end = start;
                while let Some((at, digit)) = chars.peek().copied() {
                    if !digit.is_ascii_digit() {
                        break;
                    }
                    end = at + 1;
                    let _digit = chars.next();
                }
                if end == start {
                    text.push(character);
                    continue;
                }
                let position: usize = sql[start..end]
                    .parse()
                    .expect("a run of ASCII digits reads as a parameter number");
                let parameter = position
                    .checked_sub(1)
                    .and_then(|slot| declared.get(slot))
                    .ok_or(CallerStatementRefused::UndeclaredParameter {
                        statement: statement.name(),
                        position,
                    })?;
                text.push_str(&placeholder(
                    position,
                    *parameter,
                    sql[end..].starts_with("::"),
                ));
            }
            (true, _) | (false, _) => text.push(character),
        }
    }
    Ok(text)
}

/// The row that a caller's transaction gives for a caller statement: one
/// value per result column, in column order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallerRow {
    values: Vec<StatementValue>,
}

/// Why a row does not fit the columns of its statement: a defect of the
/// adapter that read it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CallerRowRefused {
    #[error("statement `{statement}` has no result column `{column}`")]
    NoSuchColumn {
        statement: &'static str,
        column: &'static str,
    },
    #[error("statement `{statement}` declares {declared} result columns; the row has {given}")]
    CountMismatch {
        statement: &'static str,
        declared: usize,
        given: usize,
    },
    #[error("statement `{statement}` column `{column}` is {declared:?}; the row holds {given:?}")]
    TypeMismatch {
        statement: &'static str,
        column: &'static str,
        declared: CallerType,
        given: CallerType,
    },
}

impl CallerRow {
    /// A row of `values`, in column order.
    pub fn new(values: Vec<StatementValue>) -> Self {
        Self { values }
    }

    /// The values, in column order.
    pub fn values(&self) -> &[StatementValue] {
        &self.values
    }

    /// The value of `column`, checked against the columns of `statement`.
    ///
    /// Errors: [`CallerRowRefused`] for a column the statement does not
    /// declare, a row with another count of values, or a value of another
    /// type.
    pub fn value(
        &self,
        statement: &CallerStatement,
        column: &'static str,
    ) -> Result<&StatementValue, CallerRowRefused> {
        let columns = statement.columns();
        if columns.len() != self.values.len() {
            return Err(CallerRowRefused::CountMismatch {
                statement: statement.name(),
                declared: columns.len(),
                given: self.values.len(),
            });
        }
        let (index, declared) = columns
            .iter()
            .enumerate()
            .find(|(_, declared)| declared.name == column)
            .ok_or(CallerRowRefused::NoSuchColumn {
                statement: statement.name(),
                column,
            })?;
        let value = &self.values[index];
        match value.caller_type() == declared.value_type {
            true => Ok(value),
            false => Err(CallerRowRefused::TypeMismatch {
                statement: statement.name(),
                column,
                declared: declared.value_type,
                given: value.caller_type(),
            }),
        }
    }
}

/// A transaction that the caller owns. It runs one caller statement and
/// gives its one row; it does not commit.
pub trait CallerTransaction: Send {
    /// The error of the caller's driver: the statement failed, or the
    /// result was not one row of the declared columns.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Run `statement` (its [`CallerStatement::sql`] with its
    /// [`CallerStatement::parameters`]) in this transaction, and give its
    /// one row, with one value per [`CallerStatement::columns`], decoded as
    /// the declared type (SQL `NULL` as [`StatementValue::Null`]).
    fn query_one(
        &mut self,
        statement: &CallerStatement,
    ) -> impl Future<Output = Result<CallerRow, Self::Error>> + Send;
}

/// Why a caller statement failed on the core's own transaction.
#[derive(Debug, thiserror::Error)]
pub enum OwnTransactionError {
    #[error(transparent)]
    Statement(#[from] StatementFailure),
    #[error(transparent)]
    RowDecode(#[from] RowDecodeError),
}

/// The core's own path: its transaction runs the statement with the core's
/// typed parameters (the frozen text, not the cast text: the core's driver
/// sends the types).
impl CallerTransaction for Transaction<'_> {
    type Error = OwnTransactionError;

    async fn query_one(
        &mut self,
        statement: &CallerStatement,
    ) -> Result<CallerRow, OwnTransactionError> {
        let mut binder = statement.statement.bind();
        for value in statement.parameters() {
            binder = bind_value(binder, value);
        }
        let bound = binder.finish();
        let rows = self
            .query(&bound, StatementBound::ConnectionTimeouts)
            .await?;
        let row = one_row(rows, statement.statement)?;
        Ok(decode_row(&row, statement)?)
    }
}

static NULL_BOOL: Option<bool> = None;
static NULL_INT2: Option<i16> = None;
static NULL_INT4: Option<i32> = None;
static NULL_INT8: Option<i64> = None;
static NULL_TEXT: Option<String> = None;
static NULL_BYTEA: Option<Vec<u8>> = None;
static NULL_UUID: Option<Uuid> = None;
static NULL_TIMESTAMPTZ: Option<DateTime<Utc>> = None;

/// Bind one value with the Rust type of its caller type.
fn bind_value<'a>(binder: ParameterBinder<'a>, value: &'a StatementValue) -> ParameterBinder<'a> {
    match value {
        StatementValue::Bool(value) => binder.value(value),
        StatementValue::Int2(value) => binder.value(value),
        StatementValue::Int4(value) => binder.value(value),
        StatementValue::Int8(value) => binder.value(value),
        StatementValue::Text(value) => binder.value(value),
        StatementValue::Bytea(value) => binder.value(value),
        StatementValue::Uuid(value) => binder.value(value),
        StatementValue::Timestamptz(value) => binder.value(value),
        StatementValue::Null(CallerType::Bool) => binder.value(&NULL_BOOL),
        StatementValue::Null(CallerType::Int2) => binder.value(&NULL_INT2),
        StatementValue::Null(CallerType::Int4) => binder.value(&NULL_INT4),
        StatementValue::Null(CallerType::Int8) => binder.value(&NULL_INT8),
        StatementValue::Null(CallerType::Text) => binder.value(&NULL_TEXT),
        StatementValue::Null(CallerType::Bytea) => binder.value(&NULL_BYTEA),
        StatementValue::Null(CallerType::Uuid) => binder.value(&NULL_UUID),
        StatementValue::Null(CallerType::Timestamptz) => binder.value(&NULL_TIMESTAMPTZ),
    }
}

/// Decode a row of the core's driver as the declared columns.
///
/// Errors: [`RowDecodeError::Column`] for a column that is missing or of
/// another type.
fn decode_row(row: &Row, statement: &CallerStatement) -> Result<CallerRow, RowDecodeError> {
    let mut values = Vec::with_capacity(statement.columns().len());
    for column in statement.columns() {
        values.push(decode_column(row, statement, column)?);
    }
    Ok(CallerRow::new(values))
}

fn decode_column(
    row: &Row,
    statement: &CallerStatement,
    column: &CallerColumn,
) -> Result<StatementValue, RowDecodeError> {
    let refused = |cause| RowDecodeError::Column {
        statement: statement.name(),
        column: column.name,
        cause,
    };
    let null = StatementValue::Null(column.value_type);
    let value = match column.value_type {
        CallerType::Bool => row
            .try_get::<_, Option<bool>>(column.name)
            .map_err(refused)?
            .map(StatementValue::Bool),
        CallerType::Int2 => row
            .try_get::<_, Option<i16>>(column.name)
            .map_err(refused)?
            .map(StatementValue::Int2),
        CallerType::Int4 => row
            .try_get::<_, Option<i32>>(column.name)
            .map_err(refused)?
            .map(StatementValue::Int4),
        CallerType::Int8 => row
            .try_get::<_, Option<i64>>(column.name)
            .map_err(refused)?
            .map(StatementValue::Int8),
        CallerType::Text => row
            .try_get::<_, Option<String>>(column.name)
            .map_err(refused)?
            .map(StatementValue::Text),
        CallerType::Bytea => row
            .try_get::<_, Option<Vec<u8>>>(column.name)
            .map_err(refused)?
            .map(StatementValue::Bytea),
        CallerType::Uuid => row
            .try_get::<_, Option<Uuid>>(column.name)
            .map_err(refused)?
            .map(StatementValue::Uuid),
        CallerType::Timestamptz => row
            .try_get::<_, Option<DateTime<Utc>>>(column.name)
            .map_err(refused)?
            .map(StatementValue::Timestamptz),
    };
    Ok(value.unwrap_or(null))
}

#[cfg(test)]
mod tests {
    use super::*;

    static READ: TypedStatement = TypedStatement::new(
        "caller_read",
        "SELECT outcome, task_id FROM some_function($1, $2, 'a $3 in a literal', $3::bigint)",
        &[
            ParameterType::Uuid,
            ParameterType::Text,
            ParameterType::Int8,
        ],
    );

    static COLUMNS: [CallerColumn; 2] = [
        CallerColumn {
            name: "outcome",
            value_type: CallerType::Text,
        },
        CallerColumn {
            name: "task_id",
            value_type: CallerType::Uuid,
        },
    ];

    fn read_values() -> Vec<StatementValue> {
        vec![
            StatementValue::Uuid(Uuid::nil()),
            StatementValue::Null(CallerType::Text),
            StatementValue::Int8(7),
        ]
    }

    /// Each parameter gets its cast; a cast already written and a `$n` in a
    /// quoted literal stay as they are.
    #[test]
    fn the_positional_text_casts_each_parameter_once() {
        assert_eq!(
            positional_sql(&READ),
            Ok(
                "SELECT outcome, task_id FROM some_function($1::uuid, $2::text, \
                 'a $3 in a literal', $3::bigint)"
                    .to_owned()
            )
        );
    }

    /// The named-bind text casts each bind; a cast already written stays
    /// after it, and a `$n` in a quoted literal stays as it is.
    #[test]
    fn the_named_text_binds_and_casts_each_parameter() {
        assert_eq!(
            named_bind_sql(&READ),
            Ok(
                "SELECT outcome, task_id FROM some_function(CAST(:p1 AS uuid), \
                 CAST(:p2 AS text), 'a $3 in a literal', CAST(:p3 AS int8)::bigint)"
                    .to_owned()
            )
        );
        assert_eq!(named_bind(3), "p3");
        let built = CallerStatement::new(&READ, read_values(), &COLUMNS).expect("the values fit");
        assert!(built.named_sql().contains("CAST(:p1 AS uuid)"));
    }

    /// The first statement of a rerun renders with a cast on both of its
    /// parameters.
    #[test]
    fn the_rerun_source_read_renders_positionally() {
        let read = super::super::operations::rerun_first_statement();
        let text = positional_sql(read).expect("the statement declares its parameters");
        assert!(
            text.ends_with("FROM horsies_rerun_source_read($1::uuid, $2::text)"),
            "{text}"
        );
    }

    #[test]
    fn a_statement_with_values_that_fit_is_built() {
        let built = CallerStatement::new(&READ, read_values(), &COLUMNS).expect("the values fit");
        assert_eq!(built.name(), "caller_read");
        assert_eq!(built.parameters(), read_values().as_slice());
        assert_eq!(built.columns(), &COLUMNS);
        assert!(built.sql().contains("$1::uuid"));
    }

    #[test]
    fn values_that_do_not_fit_are_refused_with_the_statement() {
        assert_eq!(
            CallerStatement::new(&READ, vec![StatementValue::Uuid(Uuid::nil())], &COLUMNS),
            Err(CallerStatementRefused::CountMismatch {
                statement: "caller_read",
                declared: 3,
                given: 1,
            })
        );
        let mut wrong = read_values();
        wrong[1] = StatementValue::Int4(1);
        assert_eq!(
            CallerStatement::new(&READ, wrong, &COLUMNS),
            Err(CallerStatementRefused::TypeMismatch {
                statement: "caller_read",
                position: 2,
                declared: "text",
                given: CallerType::Int4,
            })
        );
    }

    #[test]
    fn a_json_parameter_is_not_a_caller_type() {
        static JSON: TypedStatement =
            TypedStatement::new("json", "SELECT $1", &[ParameterType::Jsonb]);
        assert_eq!(
            CallerStatement::new(&JSON, vec![StatementValue::Null(CallerType::Text)], &[]),
            Err(CallerStatementRefused::UnsupportedType {
                statement: "json",
                position: 1,
                declared: "jsonb",
            })
        );
    }

    #[test]
    fn a_parameter_the_statement_does_not_declare_is_refused() {
        static PAST: TypedStatement =
            TypedStatement::new("past", "SELECT $1, $2", &[ParameterType::Bool]);
        assert_eq!(
            positional_sql(&PAST),
            Err(CallerStatementRefused::UndeclaredParameter {
                statement: "past",
                position: 2,
            })
        );
    }

    #[test]
    fn a_row_value_is_read_by_column_and_checked() {
        let built = CallerStatement::new(&READ, read_values(), &COLUMNS).expect("the values fit");
        let row = CallerRow::new(vec![
            StatementValue::Text("INSERTED".to_owned()),
            StatementValue::Uuid(Uuid::nil()),
        ]);
        assert_eq!(
            row.value(&built, "outcome"),
            Ok(&StatementValue::Text("INSERTED".to_owned()))
        );
        assert_eq!(
            row.value(&built, "missing"),
            Err(CallerRowRefused::NoSuchColumn {
                statement: "caller_read",
                column: "missing",
            })
        );
        let wrong = CallerRow::new(vec![
            StatementValue::Int4(1),
            StatementValue::Uuid(Uuid::nil()),
        ]);
        assert_eq!(
            wrong.value(&built, "outcome"),
            Err(CallerRowRefused::TypeMismatch {
                statement: "caller_read",
                column: "outcome",
                declared: CallerType::Text,
                given: CallerType::Int4,
            })
        );
        let short = CallerRow::new(vec![StatementValue::Null(CallerType::Text)]);
        assert_eq!(
            short.value(&built, "outcome"),
            Err(CallerRowRefused::CountMismatch {
                statement: "caller_read",
                declared: 2,
                given: 1,
            })
        );
    }
}
