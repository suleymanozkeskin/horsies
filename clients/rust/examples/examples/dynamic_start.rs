//! A task that builds a workflow at run time and starts it: the region of
//! the report is the task's input. The task does not wait for the workflow
//! (a wait inside a task is refused); it returns the workflow id, and the
//! demo reads the workflow's result by that id.
//!
//! ```text
//! cargo run -p horsies-examples --example dynamic_start -- worker --profile default
//! cargo run -p horsies-examples --example dynamic_start -- demo
//! ```

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::process::ExitCode;
use std::time::Duration;

use horsies::{
    task, AppConfig, CustomErrorCode, Horsies, HorsiesError, TaskError, TaskOutcome, TaskRuntime,
    WaitLimit, WorkflowSpec, WorkflowSpecBuilder,
};
use horsies_examples::{
    app_refused, database_url, isolated_profile, main_of, DemoResult, DEFAULT_PROFILE,
};
use uuid::Uuid;

/// The codes of `start_report`'s own errors.
#[derive(Debug, Clone, PartialEq, CustomErrorCode)]
enum ReportCode {
    SpecRefused,
    StartFailed,
}

#[task("fetch_sales")]
async fn fetch_sales(region: String) -> Result<u64, TaskError<Infallible>> {
    Ok(u64::try_from(region.len()).unwrap_or(0) * 1_000)
}

#[task("format_report")]
async fn format_report(
    sales: TaskOutcome<u64, Infallible>,
) -> Result<String, TaskError<Infallible>> {
    match sales {
        TaskOutcome::Succeeded(cents) => Ok(format!("sales: {cents} cents")),
        TaskOutcome::Failed(error) => Ok(format!("no sales figure: {error}")),
    }
}

/// The spec of the report of `region`, built at run time on `builder`, a
/// workflow builder of the app.
fn report_spec(
    mut builder: WorkflowSpecBuilder,
    region: String,
) -> Result<WorkflowSpec, HorsiesError> {
    builder.definition_key("examples.regional_report.v1");
    let sales = builder.task(fetch_sales::node().set(fetch_sales::fields::region(), region)?)?;
    let report =
        builder.task(format_report::node().arg_from(format_report::fields::sales(), sales))?;
    builder.output(report);
    builder.build()
}

#[task("start_report")]
async fn start_report(rt: TaskRuntime, region: String) -> Result<Uuid, TaskError<ReportCode>> {
    // The runtime gives a builder of the app that runs the task.
    let spec = report_spec(rt.workflow_builder("regional_report"), region)
        .map_err(|refused| TaskError::custom(ReportCode::SpecRefused, refused.to_string()))?;
    let handle = rt
        .start::<String>(&spec)
        .await
        .map_err(|error| TaskError::custom(ReportCode::StartFailed, error.to_string()))?;
    Ok(handle.workflow_id())
}

fn build_app() -> Result<Horsies, HorsiesError> {
    let mut config = AppConfig::from_database_url(database_url());
    config.worker_profiles = BTreeMap::from([(DEFAULT_PROFILE.to_owned(), isolated_profile())]);
    let mut app = Horsies::new(config)?;
    let _fetch_sales = fetch_sales::register(&mut app)?;
    let _format_report = format_report::register(&mut app)?;
    let _start_report = start_report::register(&mut app)?;
    Ok(app)
}

async fn demo(app: Horsies) -> DemoResult {
    let limit = WaitLimit::Within(Duration::from_secs(60));
    let started = start_report::send(&app, "emea".to_owned()).await?;
    let workflow_id = match started.get(limit).await? {
        TaskOutcome::Succeeded(workflow_id) => workflow_id,
        TaskOutcome::Failed(error) => {
            println!("start_report failed: {error}");
            return Ok(());
        }
    };
    println!("start_report started workflow {workflow_id}");
    match app
        .workflow_handle::<String>(workflow_id)
        .get(limit)
        .await?
    {
        TaskOutcome::Succeeded(report) => println!("{report}"),
        TaskOutcome::Failed(error) => println!("the report failed: {error}"),
    }
    Ok(())
}

fn main() -> ExitCode {
    match build_app() {
        Ok(app) => main_of(app, demo),
        Err(error) => app_refused(&error),
    }
}
