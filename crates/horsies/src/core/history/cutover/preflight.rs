//! Read-only inventory and bounded total-window estimate.

use sqlx::{FromRow, PgConnection};

use crate::broker::migrations::{expected_schema_version, MIGRATIONS_TABLE};
use crate::core::history::ddl::classes::FOREVER_CLASS_KEY;
use crate::core::history::errors::HistoryError;
use crate::core::history::names::{HEARTBEATS_TABLE, LIVE_TASKS, TASK_HISTORY_PARENT};

pub const PLANNING_CEILING_NUMERATOR: u64 = 5;
pub const PLANNING_CEILING_DENOMINATOR: u64 = 4;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RelocationCoefficients {
    seconds_per_million_rows: f64,
    fixed_seconds: f64,
    preparation_seconds_per_million_rows: f64,
}

impl RelocationCoefficients {
    pub fn new(
        seconds_per_million_rows: f64,
        fixed_seconds: f64,
        preparation_seconds_per_million_rows: f64,
    ) -> Result<Self, PreflightError> {
        let values = [
            seconds_per_million_rows,
            fixed_seconds,
            preparation_seconds_per_million_rows,
        ];
        if values
            .iter()
            .any(|value| !value.is_finite() || *value < 0.0)
        {
            return Err(PreflightError::InvalidCoefficients);
        }
        Ok(Self {
            seconds_per_million_rows,
            fixed_seconds,
            preparation_seconds_per_million_rows,
        })
    }

    pub fn seconds_per_million_rows(self) -> f64 {
        self.seconds_per_million_rows
    }

    pub fn fixed_seconds(self) -> f64 {
        self.fixed_seconds
    }

    pub fn preparation_seconds_per_million_rows(self) -> f64 {
        self.preparation_seconds_per_million_rows
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CutoverEstimate {
    pub coefficients: RelocationCoefficients,
    pub rows: i64,
    pub preparation_seconds: f64,
    pub relocation_seconds: f64,
    pub stage_seconds: Vec<(String, f64)>,
    pub total_seconds: f64,
    pub ceiling_seconds: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CutoverPreflight {
    pub stored_schema_version: i64,
    pub history_parent_present: bool,
    pub terminal_live_rows: i64,
    pub unrecorded_kind_rows: i64,
    pub unfingerprinted_rows: i64,
    pub unprepared_envelope_rows: i64,
    pub unclassified_rows: i64,
    pub unclassified_live_bytes: i64,
    pub class_day_pairs: i64,
    pub workflow_rows: i64,
    pub heartbeat_rows: i64,
    pub estimate: CutoverEstimate,
    pub advisories: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum PreflightError {
    #[error("relocation coefficients must be finite and non-negative")]
    InvalidCoefficients,
    #[error("stage estimates must be finite and non-negative")]
    InvalidStageEstimate,
    #[error("stored schema version {stored} predates {required}; run the ordinary migration chain first")]
    StaleSchema { stored: i64, required: i64 },
    #[error("the emitted history foundation is absent")]
    HistoryFoundationAbsent,
    #[error(transparent)]
    History(#[from] HistoryError),
}

pub fn estimate_relocation(
    coefficients: RelocationCoefficients,
    rows: i64,
    stage_seconds: Vec<(String, f64)>,
) -> Result<CutoverEstimate, PreflightError> {
    if rows < 0
        || stage_seconds
            .iter()
            .any(|(_, seconds)| !seconds.is_finite() || *seconds < 0.0)
    {
        return Err(PreflightError::InvalidStageEstimate);
    }
    let millions = rows as f64 / 1_000_000.0;
    let preparation = coefficients.preparation_seconds_per_million_rows * millions;
    let relocation = coefficients.seconds_per_million_rows * millions;
    let total = coefficients.fixed_seconds
        + preparation
        + relocation
        + stage_seconds
            .iter()
            .map(|(_, seconds)| seconds)
            .sum::<f64>();
    Ok(CutoverEstimate {
        coefficients,
        rows,
        preparation_seconds: preparation,
        relocation_seconds: relocation,
        stage_seconds,
        total_seconds: total,
        ceiling_seconds: total * PLANNING_CEILING_NUMERATOR as f64
            / PLANNING_CEILING_DENOMINATOR as f64,
    })
}

#[derive(FromRow)]
struct Inventory {
    terminal_rows: i64,
    unrecorded_kind_rows: i64,
    unfingerprinted_rows: i64,
    unprepared_rows: i64,
    unclassified_rows: i64,
    unclassified_bytes: i64,
    class_day_pairs: i64,
}

pub async fn run_preflight(
    connection: &mut PgConnection,
    coefficients: RelocationCoefficients,
) -> Result<CutoverPreflight, PreflightError> {
    let stored: i64 = sqlx::query_scalar(&format!(
        "SELECT COALESCE(max(version), 0) FROM {MIGRATIONS_TABLE} WHERE success"
    ))
    .fetch_one(&mut *connection)
    .await
    .map_err(HistoryError::from)?;
    let required = expected_schema_version();
    if stored < required {
        return Err(PreflightError::StaleSchema { stored, required });
    }
    let history_parent_present: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(TASK_HISTORY_PARENT)
        .fetch_one(&mut *connection)
        .await
        .map_err(HistoryError::from)?;
    if !history_parent_present {
        return Err(PreflightError::HistoryFoundationAbsent);
    }

    let inventory: Inventory = sqlx::query_as(&format!(
        "SELECT
             count(*) FILTER (WHERE terminal) AS terminal_rows,
             count(*) FILTER (
                 WHERE terminal AND terminalization_kind IS NULL
             ) AS unrecorded_kind_rows,
             count(*) FILTER (
                 WHERE terminal AND command_fingerprint IS NULL
             ) AS unfingerprinted_rows,
             count(*) FILTER (
                 WHERE terminal AND prepared_rerun_input_disposition IS NULL
             ) AS unprepared_rows,
             count(*) FILTER (
                 WHERE terminal AND retention_class_key IS NULL
             ) AS unclassified_rows,
             COALESCE(sum(row_bytes) FILTER (
                 WHERE terminal AND retention_class_key IS NULL
             ), 0) AS unclassified_bytes,
             count(DISTINCT CASE WHEN terminal THEN
                 (retention_class_key,
                  date_trunc('day', terminal_at AT TIME ZONE 'UTC'))
             END) AS class_day_pairs
         FROM (
             SELECT *, status NOT IN ('PENDING', 'CLAIMED', 'RUNNING') AS terminal,
                    pg_column_size({LIVE_TASKS}) AS row_bytes
             FROM {LIVE_TASKS}
         ) AS rows"
    ))
    .fetch_one(&mut *connection)
    .await
    .map_err(HistoryError::from)?;
    let (workflow_rows, heartbeat_rows): (i64, i64) = sqlx::query_as(&format!(
        "SELECT (SELECT count(*) FROM horsies_workflows),
                (SELECT count(*) FROM {HEARTBEATS_TABLE})"
    ))
    .fetch_one(connection)
    .await
    .map_err(HistoryError::from)?;

    let mut advisories = Vec::new();
    if inventory.unclassified_rows != 0 {
        let megabytes = inventory.unclassified_bytes as f64 / (1024.0 * 1024.0);
        advisories.push(format!(
            "{} terminal rows ({megabytes:.1} MB live) carry no retention class; \
             relocation will place them in the '{FOREVER_CLASS_KEY}' class (no automatic \
             aging); backfill a class before cutover to age them",
            inventory.unclassified_rows
        ));
    }
    Ok(CutoverPreflight {
        stored_schema_version: stored,
        history_parent_present,
        terminal_live_rows: inventory.terminal_rows,
        unrecorded_kind_rows: inventory.unrecorded_kind_rows,
        unfingerprinted_rows: inventory.unfingerprinted_rows,
        unprepared_envelope_rows: inventory.unprepared_rows,
        unclassified_rows: inventory.unclassified_rows,
        unclassified_live_bytes: inventory.unclassified_bytes,
        class_day_pairs: inventory.class_day_pairs,
        workflow_rows,
        heartbeat_rows,
        estimate: estimate_relocation(coefficients, inventory.terminal_rows, Vec::new())?,
        advisories,
    })
}
