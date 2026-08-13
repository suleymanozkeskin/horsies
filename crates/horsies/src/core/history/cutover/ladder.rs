//! Ladder fit and the ruled lower/upper prediction bounds.

use super::preflight::{
    estimate_relocation, CutoverEstimate, PreflightError, RelocationCoefficients,
};

pub const RUNG_FLOOR_NUMERATOR: u64 = 7;
pub const RUNG_FLOOR_DENOMINATOR: u64 = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LadderRung {
    pub name: &'static str,
    pub rows: i64,
    pub contingent: bool,
}

pub const LADDER: [LadderRung; 3] = [
    LadderRung {
        name: "one-million",
        rows: 1_000_000,
        contingent: false,
    },
    LadderRung {
        name: "ten-million",
        rows: 10_000_000,
        contingent: false,
    },
    LadderRung {
        name: "hundred-million",
        rows: 100_000_000,
        contingent: true,
    },
];

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BatchCommit {
    pub cumulative_rows: i64,
    pub elapsed_seconds: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MeasuredRun {
    pub rows: i64,
    pub seconds: f64,
    pub fixed_seconds: f64,
    pub preparation_seconds: f64,
    pub commits: Vec<BatchCommit>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FittedRun {
    pub coefficients: RelocationCoefficients,
    pub regression_intercept_seconds: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RungOutcome {
    Passed {
        rung: LadderRung,
        estimate: CutoverEstimate,
        measured_seconds: f64,
        refit: FittedRun,
    },
    Busted {
        rung: LadderRung,
        estimate: CutoverEstimate,
        measured_seconds: f64,
    },
    Overpredicted {
        rung: LadderRung,
        estimate: CutoverEstimate,
        measured_seconds: f64,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum LadderError {
    #[error("the slope regression requires at least two distinct batch-commit points")]
    InsufficientDistinctCommits,
    #[error("ladder measurements must be finite, positive, and match the rung row count")]
    InvalidMeasurement,
    #[error(transparent)]
    Preflight(#[from] PreflightError),
}

pub fn fit_run(run: &MeasuredRun) -> Result<FittedRun, LadderError> {
    if run.rows <= 0
        || !run.seconds.is_finite()
        || run.seconds < 0.0
        || !run.fixed_seconds.is_finite()
        || run.fixed_seconds < 0.0
        || !run.preparation_seconds.is_finite()
        || run.preparation_seconds < 0.0
        || run.commits.iter().any(|commit| {
            commit.cumulative_rows <= 0
                || !commit.elapsed_seconds.is_finite()
                || commit.elapsed_seconds < 0.0
        })
    {
        return Err(LadderError::InvalidMeasurement);
    }
    let mut distinct = std::collections::BTreeSet::new();
    distinct.extend(run.commits.iter().map(|commit| commit.cumulative_rows));
    if distinct.len() < 2 {
        return Err(LadderError::InsufficientDistinctCommits);
    }
    let count = run.commits.len() as f64;
    let mean_x = run
        .commits
        .iter()
        .map(|commit| commit.cumulative_rows as f64 / 1_000_000.0)
        .sum::<f64>()
        / count;
    let mean_y = run
        .commits
        .iter()
        .map(|commit| commit.elapsed_seconds)
        .sum::<f64>()
        / count;
    let denominator = run
        .commits
        .iter()
        .map(|commit| {
            let x = commit.cumulative_rows as f64 / 1_000_000.0;
            (x - mean_x).powi(2)
        })
        .sum::<f64>();
    if denominator == 0.0 {
        return Err(LadderError::InsufficientDistinctCommits);
    }
    let slope = run
        .commits
        .iter()
        .map(|commit| {
            let x = commit.cumulative_rows as f64 / 1_000_000.0;
            (x - mean_x) * (commit.elapsed_seconds - mean_y)
        })
        .sum::<f64>()
        / denominator;
    let intercept = mean_y - slope * mean_x;
    Ok(FittedRun {
        coefficients: RelocationCoefficients::new(
            slope,
            run.fixed_seconds,
            run.preparation_seconds / (run.rows as f64 / 1_000_000.0),
        )?,
        regression_intercept_seconds: intercept,
    })
}

pub fn evaluate_rung(
    rung: LadderRung,
    coefficients: RelocationCoefficients,
    measured: &MeasuredRun,
) -> Result<RungOutcome, LadderError> {
    if measured.rows != rung.rows {
        return Err(LadderError::InvalidMeasurement);
    }
    let estimate = estimate_relocation(coefficients, rung.rows, Vec::new())?;
    if measured.seconds > estimate.ceiling_seconds {
        return Ok(RungOutcome::Busted {
            rung,
            estimate,
            measured_seconds: measured.seconds,
        });
    }
    let floor =
        estimate.total_seconds * RUNG_FLOOR_NUMERATOR as f64 / RUNG_FLOOR_DENOMINATOR as f64;
    if measured.seconds < floor {
        return Ok(RungOutcome::Overpredicted {
            rung,
            estimate,
            measured_seconds: measured.seconds,
        });
    }
    Ok(RungOutcome::Passed {
        rung,
        estimate,
        measured_seconds: measured.seconds,
        refit: fit_run(measured)?,
    })
}
