//! Typed statements: SQL text plus the types of its parameters.
//!
//! Rules of the statement layer:
//! - Parameter types are built-in PostgreSQL types only ([`ParameterType`] is
//!   closed). Result columns must be built-in types too: a function that
//!   returns a composite is called as `SELECT * FROM fn(...)`. A custom type in
//!   a result column makes the driver look the type up with a named prepared
//!   statement, which PgBouncer transaction mode cannot carry.
//! - [`StatementProtocol::PreparedPerConnection`] (direct and session
//!   connections) prepares each statement once per connection and then runs
//!   it in one round trip.
//! - [`StatementProtocol::UnnamedOnly`] (PgBouncer transaction mode) sends
//!   Parse, Bind, Describe, Execute and Sync in one write with the unnamed
//!   statement (`query_typed`, `execute_typed`). No named statement is ever
//!   prepared in that mode.

use std::time::Duration;

use deadpool_postgres::ClientWrapper;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::Row;

/// The time the core waits for the reply of a cancelled statement after its
/// cancel request. When it passes, the connection is discarded and the
/// outcome is unknown.
pub const CANCEL_GRACE: Duration = Duration::from_secs(5);

/// The built-in PostgreSQL types a statement parameter may have.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ParameterType {
    Bool,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Text,
    Bytea,
    Uuid,
    Json,
    Jsonb,
    Timestamptz,
    BoolArray,
    Int2Array,
    Int4Array,
    Int8Array,
    Float8Array,
    TextArray,
    ByteaArray,
    UuidArray,
    TimestamptzArray,
    JsonbArray,
}

impl ParameterType {
    /// The driver's type value.
    pub fn postgres_type(self) -> Type {
        match self {
            ParameterType::Bool => Type::BOOL,
            ParameterType::Int2 => Type::INT2,
            ParameterType::Int4 => Type::INT4,
            ParameterType::Int8 => Type::INT8,
            ParameterType::Float4 => Type::FLOAT4,
            ParameterType::Float8 => Type::FLOAT8,
            ParameterType::Text => Type::TEXT,
            ParameterType::Bytea => Type::BYTEA,
            ParameterType::Uuid => Type::UUID,
            ParameterType::Json => Type::JSON,
            ParameterType::Jsonb => Type::JSONB,
            ParameterType::Timestamptz => Type::TIMESTAMPTZ,
            ParameterType::BoolArray => Type::BOOL_ARRAY,
            ParameterType::Int2Array => Type::INT2_ARRAY,
            ParameterType::Int4Array => Type::INT4_ARRAY,
            ParameterType::Int8Array => Type::INT8_ARRAY,
            ParameterType::Float8Array => Type::FLOAT8_ARRAY,
            ParameterType::TextArray => Type::TEXT_ARRAY,
            ParameterType::ByteaArray => Type::BYTEA_ARRAY,
            ParameterType::UuidArray => Type::UUID_ARRAY,
            ParameterType::TimestamptzArray => Type::TIMESTAMPTZ_ARRAY,
            ParameterType::JsonbArray => Type::JSONB_ARRAY,
        }
    }

    /// The SQL spelling, for messages.
    pub fn sql_name(self) -> &'static str {
        match self {
            ParameterType::Bool => "bool",
            ParameterType::Int2 => "int2",
            ParameterType::Int4 => "int4",
            ParameterType::Int8 => "int8",
            ParameterType::Float4 => "float4",
            ParameterType::Float8 => "float8",
            ParameterType::Text => "text",
            ParameterType::Bytea => "bytea",
            ParameterType::Uuid => "uuid",
            ParameterType::Json => "json",
            ParameterType::Jsonb => "jsonb",
            ParameterType::Timestamptz => "timestamptz",
            ParameterType::BoolArray => "bool[]",
            ParameterType::Int2Array => "int2[]",
            ParameterType::Int4Array => "int4[]",
            ParameterType::Int8Array => "int8[]",
            ParameterType::Float8Array => "float8[]",
            ParameterType::TextArray => "text[]",
            ParameterType::ByteaArray => "bytea[]",
            ParameterType::UuidArray => "uuid[]",
            ParameterType::TimestamptzArray => "timestamptz[]",
            ParameterType::JsonbArray => "jsonb[]",
        }
    }
}

/// One SQL statement with the types of its parameters (`$1` is the first
/// type). Statements are constants:
///
/// ```ignore
/// const CLAIM_PROBE: TypedStatement = TypedStatement::new(
///     "claim_probe",
///     "SELECT count(*) FROM horsies_tasks WHERE queue_name = $1",
///     &[ParameterType::Text],
/// );
/// ```
#[derive(Debug)]
pub struct TypedStatement {
    name: &'static str,
    sql: &'static str,
    parameter_types: &'static [ParameterType],
}

impl TypedStatement {
    /// `name` names the statement in errors and logs.
    pub const fn new(
        name: &'static str,
        sql: &'static str,
        parameter_types: &'static [ParameterType],
    ) -> TypedStatement {
        TypedStatement {
            name,
            sql,
            parameter_types,
        }
    }

    pub const fn name(&self) -> &'static str {
        self.name
    }

    pub const fn sql(&self) -> &'static str {
        self.sql
    }

    pub const fn parameter_types(&self) -> &'static [ParameterType] {
        self.parameter_types
    }

    /// Starts binding parameter values, in order.
    pub fn bind(&self) -> ParameterBinder<'_> {
        ParameterBinder {
            statement: self,
            values: Vec::with_capacity(self.parameter_types.len()),
        }
    }
}

/// Binds values to a statement's parameters, in order.
///
/// A value whose Rust type does not encode as the declared type, a value past
/// the declared count, and a missing value are defects of the caller: the
/// binder panics with the statement name, the position and both types.
pub struct ParameterBinder<'a> {
    statement: &'a TypedStatement,
    values: Vec<&'a (dyn ToSql + Sync)>,
}

impl<'a> ParameterBinder<'a> {
    /// Binds the next parameter.
    ///
    /// Panics when the statement declares no further parameter, or when `T`
    /// does not encode as the declared type.
    #[must_use]
    pub fn value<T: ToSql + Sync>(mut self, value: &'a T) -> ParameterBinder<'a> {
        let position = self.values.len() + 1;
        let declared = match self.statement.parameter_types.get(position - 1) {
            Some(declared) => *declared,
            None => panic!(
                "statement `{}` declares {} parameters; ${position} has no declared type",
                self.statement.name,
                self.statement.parameter_types.len(),
            ),
        };
        assert!(
            <T as ToSql>::accepts(&declared.postgres_type()),
            "statement `{}` parameter ${position}: the Rust type `{}` does not encode as {}",
            self.statement.name,
            std::any::type_name::<T>(),
            declared.sql_name(),
        );
        self.values.push(value);
        self
    }

    /// Ends binding.
    ///
    /// Panics when fewer values than declared parameters were bound.
    pub fn finish(self) -> BoundStatement<'a> {
        assert_eq!(
            self.values.len(),
            self.statement.parameter_types.len(),
            "statement `{}` declares {} parameters and {} were bound",
            self.statement.name,
            self.statement.parameter_types.len(),
            self.values.len(),
        );
        BoundStatement {
            statement: self.statement,
            values: self.values,
        }
    }
}

/// A statement with one value per declared parameter.
pub struct BoundStatement<'a> {
    statement: &'a TypedStatement,
    values: Vec<&'a (dyn ToSql + Sync)>,
}

impl<'a> BoundStatement<'a> {
    pub fn statement(&self) -> &'a TypedStatement {
        self.statement
    }

    fn values(&self) -> &[&'a (dyn ToSql + Sync)] {
        &self.values
    }

    fn typed_values(&self) -> Vec<(&'a (dyn ToSql + Sync), Type)> {
        self.values
            .iter()
            .zip(self.statement.parameter_types)
            .map(|(value, parameter_type)| (*value, parameter_type.postgres_type()))
            .collect()
    }

    fn postgres_types(&self) -> Vec<Type> {
        self.statement
            .parameter_types
            .iter()
            .map(|parameter_type| parameter_type.postgres_type())
            .collect()
    }
}

/// A SQL text of one or more statements with no parameters, such as a
/// migration. It is sent with the simple query protocol, in one `Query`
/// message: nothing is prepared, so a script also passes PgBouncer
/// transaction mode. The server runs the statements in order and stops at the
/// first error.
///
/// A script holds no transaction control (`BEGIN`, `COMMIT`, `ROLLBACK`): a
/// [`crate::db::Transaction`] knows its state only from its own commands.
#[derive(Clone, Copy)]
pub struct SqlScript {
    name: &'static str,
    sql: &'static str,
}

/// The name and the length of the text: a migration text is too long for a
/// log line.
impl std::fmt::Debug for SqlScript {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqlScript")
            .field("name", &self.name)
            .field("bytes", &self.sql.len())
            .finish()
    }
}

impl SqlScript {
    /// `name` names the script in errors and logs.
    pub const fn new(name: &'static str, sql: &'static str) -> SqlScript {
        SqlScript { name, sql }
    }

    pub const fn name(&self) -> &'static str {
        self.name
    }

    pub const fn sql(&self) -> &'static str {
        self.sql
    }
}

/// How statements reach the server on one pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatementProtocol {
    /// Direct and session connections: prepare once per connection (cached),
    /// then one round trip per run.
    PreparedPerConnection,
    /// PgBouncer transaction mode: the unnamed statement in one write, every
    /// time. Never prepares a named statement.
    UnnamedOnly,
}

/// The bound of one statement run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatementBound {
    /// Only the connection's own bounds: the TCP user timeout and keepalives.
    /// A statement that runs long on a live connection is not bounded.
    ConnectionTimeouts,
    /// After this time the core sends a cancel request on a separate
    /// connection and waits [`CANCEL_GRACE`] for the reply.
    CancelAfter(Duration),
}

/// The step that failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Stage {
    /// The prepare round trip; the statement did not run.
    Prepare,
    /// The run of the statement.
    Run,
}

/// A driver error and the step it came from.
#[derive(Debug)]
pub(crate) struct AttemptError {
    pub(crate) stage: Stage,
    pub(crate) error: tokio_postgres::Error,
}

fn at(stage: Stage) -> impl Fn(tokio_postgres::Error) -> AttemptError {
    move |error| AttemptError { stage, error }
}

/// Runs a statement and reads every row.
pub(crate) async fn query_rows(
    client: &ClientWrapper,
    protocol: StatementProtocol,
    statement: &BoundStatement<'_>,
) -> Result<Vec<Row>, AttemptError> {
    let sql = statement.statement().sql();
    match protocol {
        StatementProtocol::UnnamedOnly => client
            .query_typed(sql, &statement.typed_values())
            .await
            .map_err(at(Stage::Run)),
        StatementProtocol::PreparedPerConnection => {
            let prepared = client
                .prepare_typed_cached(sql, &statement.postgres_types())
                .await
                .map_err(at(Stage::Prepare))?;
            client
                .query(&prepared, statement.values())
                .await
                .map_err(at(Stage::Run))
        }
    }
}

/// Runs a statement and reads the count of affected rows.
pub(crate) async fn execute_count(
    client: &ClientWrapper,
    protocol: StatementProtocol,
    statement: &BoundStatement<'_>,
) -> Result<u64, AttemptError> {
    let sql = statement.statement().sql();
    match protocol {
        StatementProtocol::UnnamedOnly => client
            .execute_typed(sql, &statement.typed_values())
            .await
            .map_err(at(Stage::Run)),
        StatementProtocol::PreparedPerConnection => {
            let prepared = client
                .prepare_typed_cached(sql, &statement.postgres_types())
                .await
                .map_err(at(Stage::Prepare))?;
            client
                .execute(&prepared, statement.values())
                .await
                .map_err(at(Stage::Run))
        }
    }
}

/// Runs one command with the simple query protocol: `BEGIN`, `COMMIT`,
/// `ROLLBACK`, the text of a [`SqlScript`], or the text of a
/// [`crate::db::PartitionDdl`]. A command has no parameter, so nothing is
/// prepared, also under PgBouncer transaction mode.
pub(crate) async fn run_command(client: &ClientWrapper, sql: &str) -> Result<(), AttemptError> {
    client.batch_execute(sql).await.map_err(at(Stage::Run))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TWO_PARAMETERS: TypedStatement = TypedStatement::new(
        "two_parameters",
        "SELECT $1::text, $2::int8",
        &[ParameterType::Text, ParameterType::Int8],
    );

    #[test]
    fn a_script_keeps_its_name_and_text() {
        const SCRIPT: SqlScript = SqlScript::new(
            "two_statements",
            "CREATE TABLE a (x int); CREATE TABLE b (y int);",
        );
        assert_eq!(SCRIPT.name(), "two_statements");
        assert_eq!(
            SCRIPT.sql(),
            "CREATE TABLE a (x int); CREATE TABLE b (y int);"
        );
        let shown = format!("{SCRIPT:?}");
        assert!(shown.contains("two_statements"), "{shown}");
        assert!(!shown.contains("CREATE TABLE"), "{shown}");
    }

    #[test]
    fn binding_in_order_keeps_the_declared_types() {
        let name = "queue".to_owned();
        let count: i64 = 3;
        let bound = TWO_PARAMETERS.bind().value(&name).value(&count).finish();
        let types: Vec<Type> = bound.typed_values().into_iter().map(|(_, t)| t).collect();
        assert_eq!(types, vec![Type::TEXT, Type::INT8]);
        assert_eq!(bound.postgres_types(), vec![Type::TEXT, Type::INT8]);
        assert_eq!(bound.values().len(), 2);
        assert_eq!(bound.statement().name(), "two_parameters");
    }

    #[test]
    #[should_panic(expected = "does not encode as int8")]
    fn a_value_of_the_wrong_type_is_a_defect() {
        let name = "queue".to_owned();
        let wrong: i32 = 3;
        let _bound = TWO_PARAMETERS.bind().value(&name).value(&wrong).finish();
    }

    #[test]
    #[should_panic(expected = "declares 2 parameters and 1 were bound")]
    fn a_missing_value_is_a_defect() {
        let name = "queue".to_owned();
        let _bound = TWO_PARAMETERS.bind().value(&name).finish();
    }

    #[test]
    #[should_panic(expected = "$3 has no declared type")]
    fn an_extra_value_is_a_defect() {
        let name = "queue".to_owned();
        let count: i64 = 3;
        let _bound = TWO_PARAMETERS
            .bind()
            .value(&name)
            .value(&count)
            .value(&count)
            .finish();
    }

    #[test]
    fn every_parameter_type_maps_to_its_driver_type() {
        let all = [
            ParameterType::Bool,
            ParameterType::Int2,
            ParameterType::Int4,
            ParameterType::Int8,
            ParameterType::Float4,
            ParameterType::Float8,
            ParameterType::Text,
            ParameterType::Bytea,
            ParameterType::Uuid,
            ParameterType::Json,
            ParameterType::Jsonb,
            ParameterType::Timestamptz,
            ParameterType::BoolArray,
            ParameterType::Int2Array,
            ParameterType::Int4Array,
            ParameterType::Int8Array,
            ParameterType::Float8Array,
            ParameterType::TextArray,
            ParameterType::ByteaArray,
            ParameterType::UuidArray,
            ParameterType::TimestamptzArray,
            ParameterType::JsonbArray,
        ];
        for parameter_type in all {
            let driver_name = parameter_type.postgres_type().name().to_owned();
            let expected = match parameter_type.sql_name().strip_suffix("[]") {
                Some(element) => format!("_{element}"),
                None => parameter_type.sql_name().to_owned(),
            };
            assert_eq!(driver_name, expected, "{parameter_type:?}");
        }
    }
}
