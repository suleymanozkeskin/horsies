//! The statement log of `echo`: one INFO event per statement on the target
//! `horsies.sql` with `echo` on, no event with `echo` off.

use super::*;
use crate::db::{ParameterType, SqlIdentifier};
use crate::test_support::{CapturedEvent, LogCapture};

const STATEMENT_LOG_TARGET: &str = "horsies.sql";

static PROBE: TypedStatement = TypedStatement::new(
    "statement_log_probe",
    "SELECT task_id FROM horsies_tasks WHERE queue_name = $1",
    &[ParameterType::Text],
);

fn detach_ddl() -> PartitionDdl {
    PartitionDdl::DetachConcurrently {
        parent: SqlIdentifier::parse("horsies_task_history").expect("a valid identifier"),
        leaf: SqlIdentifier::parse("horsies_task_history_p20261117").expect("a valid identifier"),
    }
}

/// The one event of `capture`, which must be an INFO event of the statement
/// log: the SQL text is its message, and its only field is the statement
/// name (no bound value).
fn the_one_statement_event(capture: &LogCapture) -> CapturedEvent {
    let [event]: [CapturedEvent; 1] = capture
        .events()
        .try_into()
        .expect("exactly one event per statement");
    assert_eq!(event.level, tracing::Level::INFO);
    assert_eq!(event.target, STATEMENT_LOG_TARGET);
    event
}

#[test]
fn echo_on_logs_each_statement_at_info_on_horsies_sql() {
    let capture = LogCapture::new();
    let _default = capture.set_default();

    StatementLog::from_echo(true).log(&PROBE);

    let event = the_one_statement_event(&capture);
    assert_eq!(event.message, PROBE.sql());
    assert_eq!(
        event.fields,
        vec![("statement".to_owned(), "statement_log_probe".to_owned())]
    );
}

#[test]
fn echo_on_logs_each_partition_ddl_at_info_on_horsies_sql() {
    let capture = LogCapture::new();
    let _default = capture.set_default();
    let ddl = detach_ddl();

    StatementLog::from_echo(true).log_ddl(&ddl);

    let event = the_one_statement_event(&capture);
    assert_eq!(event.message, ddl.sql());
    assert_eq!(
        event.fields,
        vec![(
            "statement".to_owned(),
            "detach_partition_concurrently".to_owned()
        )]
    );
}

#[test]
fn echo_off_logs_no_statement() {
    let capture = LogCapture::new();
    let _default = capture.set_default();

    StatementLog::from_echo(false).log(&PROBE);
    StatementLog::from_echo(false).log_ddl(&detach_ddl());

    assert_eq!(capture.events(), Vec::<CapturedEvent>::new());
}
