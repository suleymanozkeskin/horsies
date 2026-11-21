//! The scheduler's statements on `horsies_schedule_state`:
//!
//! - the due read, the start read and write, the existence heal;
//! - the fire: the slot tasks and the state move in one statement
//!   (`horsies_fire_schedule`, `core/baseline/0007_scheduler_monitoring_functions.sql`).
//!
//! Each function sends one statement in autocommit on the core pool it is
//! given, on a connection that passes the reserve gate
//! ([`CheckoutClass::Batch`]): the scheduler is producer work.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::broker::core_rows::{column, StatementLog};
use crate::broker::postgres::SendCommand;
use crate::broker::{BrokerError, RowDecodeError};
use crate::db::{
    CheckoutClass, CorePool, ParameterType, Row, StatementBound, StatementFailure, Transience,
    TypedStatement,
};

/// The due read: the rows of the names in `$1` whose `next_run_at` is at or
/// before the database time, soonest first, with that database time. The
/// one-row join returns the time also when no row is due (one row with a
/// NULL name). Python `ScheduleStateManager.read_due_schedules`.
const DUE_SCHEDULES_SQL: &str = "\
SELECT statement_timestamp() AS database_now, s.schedule_name, s.next_run_at
FROM (VALUES (1)) AS one
LEFT JOIN horsies_schedule_state AS s
    ON s.schedule_name = ANY($1)
   AND s.next_run_at <= statement_timestamp()
ORDER BY s.next_run_at";

/// [`DUE_SCHEDULES_SQL`] with its parameter type: the schedule names.
static DUE_SCHEDULES: TypedStatement = TypedStatement::new(
    "schedule_due_read",
    DUE_SCHEDULES_SQL,
    &[ParameterType::TextArray],
);

/// The start read: every row's name and digest, with the database time. The
/// one-row join returns the time also for an empty table. Python
/// `ScheduleStateManager.read_start_states`.
const START_STATES_SQL: &str = "\
SELECT statement_timestamp() AS database_now, s.schedule_name, s.config_hash
FROM (VALUES (1)) AS one
LEFT JOIN horsies_schedule_state AS s ON TRUE
ORDER BY s.schedule_name";

/// [`START_STATES_SQL`]; it takes no parameter.
static START_STATES: TypedStatement =
    TypedStatement::new("schedule_start_read", START_STATES_SQL, &[]);

/// The start write: a new schedule gets its row with no run recorded; a row
/// whose digest differs gets the new `next_run_at` and digest and keeps
/// `run_count`, `last_run_at` and `last_task_id`. The WHERE clause leaves
/// alone a row that a concurrent scheduler already moved to this digest.
/// Python `ScheduleStateManager.store_start_states`.
const STORE_START_STATES_SQL: &str = "\
INSERT INTO horsies_schedule_state (
    schedule_name, last_run_at, next_run_at, last_task_id, run_count, config_hash,
    updated_at
)
SELECT u.schedule_name, NULL, u.next_run_at, NULL, 0, u.config_hash, statement_timestamp()
FROM unnest($1::text[], $2::timestamptz[], $3::bytea[])
    AS u(schedule_name, next_run_at, config_hash)
ON CONFLICT (schedule_name) DO UPDATE
SET next_run_at = EXCLUDED.next_run_at,
    config_hash = EXCLUDED.config_hash,
    updated_at = EXCLUDED.updated_at
WHERE horsies_schedule_state.config_hash <> EXCLUDED.config_hash
RETURNING schedule_name";

/// The types of the three arrays of the start write and the heal: names,
/// next runs, digests.
const STATE_WRITE_PARAMETERS: &[ParameterType] = &[
    ParameterType::TextArray,
    ParameterType::TimestamptzArray,
    ParameterType::ByteaArray,
];

/// [`STORE_START_STATES_SQL`] with its parameter types.
static STORE_START_STATES: TypedStatement = TypedStatement::new(
    "schedule_start_write",
    STORE_START_STATES_SQL,
    STATE_WRITE_PARAMETERS,
);

/// The existence heal: a row for each name that has none. A row that exists
/// is not written. Python `ScheduleStateManager.heal_missing_states`.
const HEAL_MISSING_STATES_SQL: &str = "\
INSERT INTO horsies_schedule_state (
    schedule_name, last_run_at, next_run_at, last_task_id, run_count, config_hash,
    updated_at
)
SELECT u.schedule_name, NULL, u.next_run_at, NULL, 0, u.config_hash, statement_timestamp()
FROM unnest($1::text[], $2::timestamptz[], $3::bytea[])
    AS u(schedule_name, next_run_at, config_hash)
ON CONFLICT (schedule_name) DO NOTHING
RETURNING schedule_name";

/// [`HEAL_MISSING_STATES_SQL`] with its parameter types.
static HEAL_MISSING_STATES: TypedStatement = TypedStatement::new(
    "schedule_heal",
    HEAL_MISSING_STATES_SQL,
    STATE_WRITE_PARAMETERS,
);

/// The fire: lock the state row, compare `next_run_at`, insert the slot
/// tasks, move the row, in one statement. Python `FIRE_SCHEDULE_SQL`.
const FIRE_SCHEDULE_SQL: &str = "\
SELECT row_kind, slot_at, task_id, slot_outcome
FROM horsies_fire_schedule(
    $1::text, $2::timestamptz, $3::timestamptz, $4::timestamptz[], $5::text, $6::text,
    $7::integer, $8::text, $9::text, $10::text, $11::integer, $12::text, $13::boolean,
    $14::smallint, $15::bytea
)";

/// [`FIRE_SCHEDULE_SQL`] with its parameter types, in argument order.
static FIRE_SCHEDULE: TypedStatement = TypedStatement::new(
    "schedule_fire",
    FIRE_SCHEDULE_SQL,
    &[
        ParameterType::Text,
        ParameterType::Timestamptz,
        ParameterType::Timestamptz,
        ParameterType::TimestamptzArray,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Int4,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Int4,
        ParameterType::Text,
        ParameterType::Bool,
        ParameterType::Int2,
        ParameterType::Bytea,
    ],
);

/// The SQLSTATEs of an argument that breaks a precondition of the fire
/// function (`invalid_parameter_value`, `null_value_not_allowed`): a defect
/// of the caller, not a refusal of the fire.
const CALLER_DEFECT_SQLSTATES: [&str; 2] = ["22023", "22004"];

/// One due row of the due read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueSchedule {
    pub schedule_name: String,
    pub next_run_at: DateTime<Utc>,
}

/// The due rows, soonest first, and the database time of the read. The tick
/// computes slots and the heal's first runs from `database_now`, never from
/// the host clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueScheduleRead {
    pub database_now: DateTime<Utc>,
    pub due: Vec<DueSchedule>,
}

/// The name and stored config digest of one state row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredConfigHash {
    pub schedule_name: String,
    pub config_hash: Vec<u8>,
}

/// Every state row's name and digest, and the database time of the read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartStateRead {
    pub database_now: DateTime<Utc>,
    pub rows: Vec<StoredConfigHash>,
}

/// One row for the start write or the existence heal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleStateWrite {
    pub schedule_name: String,
    pub next_run_at: DateTime<Utc>,
    pub config_hash: Vec<u8>,
}

/// What the fire did with one slot (the outcomes of
/// `horsies_insert_task_rows`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotOutcome {
    /// The slot's task row is new.
    Inserted,
    /// An earlier fire of this slot committed its task: the same command.
    SameCommand,
    /// The slot's task id holds another command; the slot counts as fired.
    PayloadMismatch,
    /// The slot's task existed and moved to history between the conflict and
    /// the read: not enqueued again and not recorded as a run.
    ConflictUnverifiable,
}

impl SlotOutcome {
    /// The outcome of a `slot_outcome` column; `None` for a text the function
    /// does not define.
    fn parse(text: &str) -> Option<SlotOutcome> {
        match text {
            "INSERTED" => Some(SlotOutcome::Inserted),
            "SAME_COMMAND" => Some(SlotOutcome::SameCommand),
            "PAYLOAD_MISMATCH" => Some(SlotOutcome::PayloadMismatch),
            "CONFLICT_UNVERIFIABLE" => Some(SlotOutcome::ConflictUnverifiable),
            _ => None,
        }
    }
}

/// One slot of an advanced fire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FiredSlot {
    pub slot_at: DateTime<Utc>,
    pub task_id: Uuid,
    pub outcome: SlotOutcome,
}

/// The arguments of one fire.
///
/// `slot_ats` strictly increase, from `expected_next_run_at` to before
/// `next_run_at` and not after the database time of the due read they were
/// computed from. Empty slots only move `next_run_at`.
#[derive(Debug, Clone, Copy)]
pub struct ScheduleFire<'a> {
    pub schedule_name: &'a str,
    pub expected_next_run_at: DateTime<Utc>,
    pub next_run_at: DateTime<Utc>,
    pub slot_ats: &'a [DateTime<Utc>],
    pub command: &'a SendCommand,
}

/// What a fire found, when the statement ran.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleFireAnswer {
    /// The slots are written and the row moved, in one commit, with each
    /// slot's outcome in slot order. `last_run_at`, `last_task_id` and
    /// `run_count` record the fire when a slot is recorded (every outcome
    /// except [`SlotOutcome::ConflictUnverifiable`]).
    Advanced { slots: Vec<FiredSlot> },
    /// The schedule has no state row. Nothing was written.
    StateMissing,
    /// The row's `next_run_at` is not the expected one: another scheduler
    /// fired, or a start moved the row. Nothing was written.
    StateMoved,
}

/// Why a fire did not answer.
#[derive(Debug, thiserror::Error)]
pub enum ScheduleFireError {
    /// A failure that may clear: a connection failure, a checkout that timed
    /// out, a lost reply. The fire may or may not have committed. The next
    /// due read shows whether the row moved; a repeat of a committed slot
    /// answers `SAME_COMMAND` or [`ScheduleFireAnswer::StateMoved`], never a
    /// second task.
    #[error("the fire of schedule '{schedule_name}' failed and may clear: {cause}")]
    OutcomeUnknown {
        schedule_name: String,
        cause: StatementFailure,
    },
    /// The database refused the fire, or the statement cannot be sent (a
    /// closed pool). Nothing committed. A repeat with the same arguments gives
    /// the same refusal, for example a CHECK violation of the slot rows.
    #[error("the fire of schedule '{schedule_name}' was refused: {cause}")]
    Refused {
        schedule_name: String,
        cause: StatementFailure,
    },
}

/// The three parallel arrays that `unnest` reads.
fn write_arrays(writes: &[ScheduleStateWrite]) -> (Vec<String>, Vec<DateTime<Utc>>, Vec<Vec<u8>>) {
    (
        writes
            .iter()
            .map(|write| write.schedule_name.clone())
            .collect(),
        writes.iter().map(|write| write.next_run_at).collect(),
        writes
            .iter()
            .map(|write| write.config_hash.clone())
            .collect(),
    )
}

/// Runs one statement on `pool` and reads every row.
async fn query(
    pool: &CorePool,
    log: StatementLog,
    statement: &crate::db::BoundStatement<'_>,
) -> Result<Vec<Row>, StatementFailure> {
    log.log(statement.statement());
    pool.query(
        CheckoutClass::Batch,
        statement,
        StatementBound::ConnectionTimeouts,
    )
    .await
}

/// The database time of the first row of a read that always returns one row
/// (the one-row join).
///
/// Panics when the read returned no row: the join makes that impossible.
fn database_now_of(rows: &[Row], statement: &TypedStatement) -> Result<DateTime<Utc>, BrokerError> {
    let row = rows.first().unwrap_or_else(|| {
        panic!(
            "statement `{}` returned no row; its one-row join always returns one",
            statement.name()
        )
    });
    Ok(column(row, statement, "database_now")?)
}

/// Read the due rows of `schedule_names` and the database time, in one
/// statement (Python `read_due_schedules`). Rows of other names are never
/// returned. Writes nothing.
///
/// Errors: [`BrokerError::Statement`]; [`BrokerError::RowDecode`] for a
/// column that does not decode.
///
/// Panics: when the read returns no row, or a row with a name and no time
/// (or a time and no name): the statement's contract is broken.
pub async fn read_due_schedules(
    pool: &CorePool,
    log: StatementLog,
    schedule_names: &[String],
) -> Result<DueScheduleRead, BrokerError> {
    let statement = &DUE_SCHEDULES;
    let bound = statement.bind().value(&schedule_names).finish();
    let rows = query(pool, log, &bound).await?;
    let database_now = database_now_of(&rows, statement)?;
    let mut due = Vec::with_capacity(rows.len());
    for row in &rows {
        let schedule_name: Option<String> = column(row, statement, "schedule_name")?;
        let next_run_at: Option<DateTime<Utc>> = column(row, statement, "next_run_at")?;
        match (schedule_name, next_run_at) {
            (Some(schedule_name), Some(next_run_at)) => due.push(DueSchedule {
                schedule_name,
                next_run_at,
            }),
            (None, None) => {}
            (schedule_name, next_run_at) => {
                panic!("the due read returned a partial row: {schedule_name:?}, {next_run_at:?}")
            }
        }
    }
    Ok(DueScheduleRead { database_now, due })
}

/// Read every state row's name and digest and the database time, in one
/// statement (Python `read_start_states`). Writes nothing.
///
/// Errors and panics: as [`read_due_schedules`].
pub async fn read_start_states(
    pool: &CorePool,
    log: StatementLog,
) -> Result<StartStateRead, BrokerError> {
    let statement = &START_STATES;
    let bound = statement.bind().finish();
    let rows = query(pool, log, &bound).await?;
    let database_now = database_now_of(&rows, statement)?;
    let mut stored = Vec::with_capacity(rows.len());
    for row in &rows {
        let schedule_name: Option<String> = column(row, statement, "schedule_name")?;
        let config_hash: Option<Vec<u8>> = column(row, statement, "config_hash")?;
        match (schedule_name, config_hash) {
            (Some(schedule_name), Some(config_hash)) => stored.push(StoredConfigHash {
                schedule_name,
                config_hash,
            }),
            (None, None) => {}
            (schedule_name, config_hash) => {
                panic!("the start read returned a partial row: {schedule_name:?}, {config_hash:?}")
            }
        }
    }
    Ok(StartStateRead {
        database_now,
        rows: stored,
    })
}

/// Runs the start write or the heal with `writes` and reads the names
/// written.
async fn write_states(
    pool: &CorePool,
    log: StatementLog,
    statement: &TypedStatement,
    writes: &[ScheduleStateWrite],
) -> Result<Vec<String>, BrokerError> {
    if writes.is_empty() {
        return Ok(Vec::new());
    }
    let (names, next_runs, hashes) = write_arrays(writes);
    let bound = statement
        .bind()
        .value(&names)
        .value(&next_runs)
        .value(&hashes)
        .finish();
    let rows = query(pool, log, &bound).await?;
    let mut written = Vec::with_capacity(rows.len());
    for row in &rows {
        written.push(column(row, statement, "schedule_name")?);
    }
    Ok(written)
}

/// Write the rows of new and changed schedules in one statement (Python
/// `store_start_states`). A new name gets a row with no run recorded; a row
/// whose digest differs gets the new `next_run_at` and digest and keeps its
/// run record; a row that holds the digest is not written. `updated_at` is
/// the statement's database time. Returns the names of the rows written.
///
/// Errors: [`BrokerError::Statement`] (nothing written, or the outcome is
/// unknown); [`BrokerError::RowDecode`].
pub async fn store_start_states(
    pool: &CorePool,
    log: StatementLog,
    writes: &[ScheduleStateWrite],
) -> Result<Vec<String>, BrokerError> {
    write_states(pool, log, &STORE_START_STATES, writes).await
}

/// Create the row of each name that has none, in one statement (Python
/// `heal_missing_states`). A row that exists is not written. `updated_at` is
/// the statement's database time. Returns the names of the rows created.
///
/// Errors: as [`store_start_states`].
pub async fn heal_missing_states(
    pool: &CorePool,
    log: StatementLog,
    writes: &[ScheduleStateWrite],
) -> Result<Vec<String>, BrokerError> {
    write_states(pool, log, &HEAL_MISSING_STATES, writes).await
}

/// Whether a failed fire is a refusal of an argument that breaks the fire
/// function's precondition (SQLSTATE 22023 or 22004).
fn is_caller_defect(failure: &StatementFailure) -> bool {
    match failure {
        StatementFailure::StatementRefused { cause, .. } => {
            CALLER_DEFECT_SQLSTATES.contains(&cause.server_error().code().code())
        }
        StatementFailure::NotSent { .. } | StatementFailure::OutcomeUnknown { .. } => false,
    }
}

/// Fire the slots of one schedule and move its state row, in one statement
/// in autocommit (Python `ScheduleStateManager.fire_schedule`).
///
/// `horsies_fire_schedule` locks the row, compares `next_run_at` with
/// `fire.expected_next_run_at`, inserts the slot tasks (task id from the
/// schedule name and the slot time, `sent_at` the slot time, the command's
/// values) and moves the row to `fire.next_run_at`. The slot rows and the
/// row move commit together or not at all.
///
/// Errors: [`ScheduleFireError::OutcomeUnknown`] for a failure that may
/// clear ([`Transience::Transient`]); [`ScheduleFireError::Refused`] for any
/// other failure.
///
/// Panics: SQLSTATE 22023 or 22004, an argument that breaks the function's
/// precondition (a defect of the caller; nothing committed); and an answer
/// outside the function's contract (no row, an unknown row kind or slot
/// outcome, slot rows that are not the fired slots).
pub async fn fire_schedule(
    pool: &CorePool,
    log: StatementLog,
    fire: &ScheduleFire<'_>,
) -> Result<ScheduleFireAnswer, ScheduleFireError> {
    let command = fire.command;
    let execution = command.execution.as_str();
    let statement = &FIRE_SCHEDULE;
    let bound = statement
        .bind()
        .value(&fire.schedule_name)
        .value(&fire.expected_next_run_at)
        .value(&fire.next_run_at)
        .value(&fire.slot_ats)
        .value(&command.task_name)
        .value(&command.queue_name)
        .value(&command.priority)
        .value(&command.stored_input)
        .value(&command.task_options)
        .value(&execution)
        .value(&command.max_retries)
        .value(&command.retention_class_key)
        .value(&command.retain_rerun_input)
        .value(&command.fingerprint_version)
        .value(&command.fingerprint)
        .finish();
    let rows = match query(pool, log, &bound).await {
        Ok(rows) => rows,
        Err(failure) if is_caller_defect(&failure) => panic!(
            "the fire of schedule '{}' broke a precondition of horsies_fire_schedule: {failure}",
            fire.schedule_name
        ),
        Err(failure) => {
            let schedule_name = fire.schedule_name.to_owned();
            return Err(match failure.transience() {
                Transience::Transient => ScheduleFireError::OutcomeUnknown {
                    schedule_name,
                    cause: failure,
                },
                Transience::Permanent => ScheduleFireError::Refused {
                    schedule_name,
                    cause: failure,
                },
            });
        }
    };
    Ok(fire_answer(fire, &rows))
}

/// A fire answer that breaks the function's contract: a defect.
fn contract_breach(schedule_name: &str, error: RowDecodeError) -> ! {
    panic!("the fire of schedule '{schedule_name}' answered a row outside its contract: {error}")
}

/// Asserts that a `STATE_MISSING` or `STATE_MOVED` answer has no slot row.
fn assert_no_slot_rows(fire: &ScheduleFire<'_>, kind: &str, slot_rows: &[Row]) {
    assert!(
        slot_rows.is_empty(),
        "the fire of schedule '{}' answered {kind} with {} slot rows",
        fire.schedule_name,
        slot_rows.len()
    );
}

/// The call outcome row, then one SLOT row per slot after ADVANCED.
///
/// Panics: an answer outside the function's contract.
fn fire_answer(fire: &ScheduleFire<'_>, rows: &[Row]) -> ScheduleFireAnswer {
    let statement = &FIRE_SCHEDULE;
    let Some((first, slot_rows)) = rows.split_first() else {
        panic!(
            "the fire of schedule '{}' answered no row",
            fire.schedule_name
        );
    };
    let kind: String = column(first, statement, "row_kind")
        .unwrap_or_else(|error| contract_breach(fire.schedule_name, error));
    match kind.as_str() {
        "ADVANCED" => {
            let slots: Vec<FiredSlot> = slot_rows
                .iter()
                .map(|row| {
                    fired_slot(fire, row)
                        .unwrap_or_else(|error| contract_breach(fire.schedule_name, error))
                })
                .collect();
            let answered: Vec<DateTime<Utc>> = slots.iter().map(|slot| slot.slot_at).collect();
            assert_eq!(
                answered.as_slice(),
                fire.slot_ats,
                "the fire of schedule '{}' answered other slots",
                fire.schedule_name
            );
            ScheduleFireAnswer::Advanced { slots }
        }
        "STATE_MISSING" => {
            assert_no_slot_rows(fire, &kind, slot_rows);
            ScheduleFireAnswer::StateMissing
        }
        "STATE_MOVED" => {
            assert_no_slot_rows(fire, &kind, slot_rows);
            ScheduleFireAnswer::StateMoved
        }
        other => panic!(
            "the fire of schedule '{}' answered row kind {other:?}",
            fire.schedule_name
        ),
    }
}

/// One SLOT row.
///
/// Errors: a column that does not decode, or is NULL.
///
/// Panics: a row kind other than `SLOT`, or an unknown slot outcome.
fn fired_slot(fire: &ScheduleFire<'_>, row: &Row) -> Result<FiredSlot, RowDecodeError> {
    let statement = &FIRE_SCHEDULE;
    let kind: String = column(row, statement, "row_kind")?;
    assert_eq!(
        kind, "SLOT",
        "the fire of schedule '{}' answered row kind {kind:?} after ADVANCED",
        fire.schedule_name
    );
    let slot_at: DateTime<Utc> = column(row, statement, "slot_at")?;
    let task_id: Uuid = column(row, statement, "task_id")?;
    let outcome_text: String = column(row, statement, "slot_outcome")?;
    let outcome = SlotOutcome::parse(&outcome_text).unwrap_or_else(|| {
        panic!(
            "the fire of schedule '{}' answered slot outcome {outcome_text:?}",
            fire.schedule_name
        )
    });
    Ok(FiredSlot {
        slot_at,
        task_id,
        outcome,
    })
}

/// Test rows of `horsies_schedule_state`, written and read with the test
/// client on the test database (seeds and checks of the tests, not scheduler statements).
#[cfg(test)]
pub(crate) mod test_rows {
    use crate::test_support::sql::{self, TestPool};
    use chrono::{DateTime, Utc};
    use uuid::Uuid;

    /// A row of `horsies_schedule_state`.
    #[derive(Debug)]
    pub(crate) struct StateRow {
        pub next_run_at: DateTime<Utc>,
        pub last_run_at: Option<DateTime<Utc>>,
        pub last_task_id: Option<Uuid>,
        pub run_count: i32,
        pub config_hash: Vec<u8>,
    }

    impl sql::FromTestRow for StateRow {
        fn from_row(row: &tokio_postgres::Row) -> Result<Self, sql::Error> {
            Ok(Self {
                next_run_at: sql::named(row, "next_run_at")?,
                last_run_at: sql::named(row, "last_run_at")?,
                last_task_id: sql::named(row, "last_task_id")?,
                run_count: sql::named(row, "run_count")?,
                config_hash: sql::named(row, "config_hash")?,
            })
        }
    }

    /// The row of `schedule_name`, if any.
    pub(crate) async fn state_row(pool: &TestPool, schedule_name: &str) -> Option<StateRow> {
        sql::query_as(
            "SELECT next_run_at, last_run_at, last_task_id, run_count, config_hash
             FROM horsies_schedule_state WHERE schedule_name = $1",
        )
        .bind(schedule_name)
        .fetch_optional(pool)
        .await
        .expect("read the schedule state row")
    }

    /// Delete the row of `schedule_name`.
    pub(crate) async fn delete_state(pool: &TestPool, schedule_name: &str) {
        let _deleted = sql::query("DELETE FROM horsies_schedule_state WHERE schedule_name = $1")
            .bind(schedule_name)
            .execute(pool)
            .await
            .expect("delete the schedule state row");
    }

    /// Replace the row of `schedule_name` with a fresh one due at
    /// `next_run_at`: no run recorded, a zero digest.
    pub(crate) async fn seed_state(
        pool: &TestPool,
        schedule_name: &str,
        next_run_at: DateTime<Utc>,
    ) {
        delete_state(pool, schedule_name).await;
        let _inserted = sql::query(
            "INSERT INTO horsies_schedule_state (
                 schedule_name, last_run_at, next_run_at, last_task_id, run_count, config_hash,
                 updated_at
             ) VALUES ($1, NULL, $2, NULL, 0, $3, statement_timestamp())",
        )
        .bind(schedule_name)
        .bind(next_run_at)
        .bind(vec![0_u8; 32])
        .execute(pool)
        .await
        .expect("seed the schedule state row");
    }

    /// Record one run on the row of `schedule_name`.
    pub(crate) async fn record_run(
        pool: &TestPool,
        schedule_name: &str,
        run_at: DateTime<Utc>,
        next_run_at: DateTime<Utc>,
        task_id: Uuid,
    ) {
        let _updated = sql::query(
            "UPDATE horsies_schedule_state
             SET last_run_at = $2, next_run_at = $3, last_task_id = $4,
                 run_count = run_count + 1, updated_at = statement_timestamp()
             WHERE schedule_name = $1",
        )
        .bind(schedule_name)
        .bind(run_at)
        .bind(next_run_at)
        .bind(task_id)
        .execute(pool)
        .await
        .expect("record a run");
    }
}

#[cfg(test)]
mod tests;
