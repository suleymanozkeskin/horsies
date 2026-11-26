//! A workflow: a graph of task nodes whose results feed later nodes.
//! `load_prices` runs first; `sum_prices` and `count_items` take its result
//! and run side by side; `summarize` takes both. The definition is built
//! and checked at registration: a field that does not exist, or a source of
//! another type, is refused there, not at run time.
//!
//! ```text
//! cargo run -p horsies-examples --example workflows -- worker --profile default
//! cargo run -p horsies-examples --example workflows -- demo
//! ```

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::process::ExitCode;
use std::time::Duration;

use horsies::{
    task, AppConfig, CustomErrorCode, Horsies, HorsiesError, TaskError, TaskOutcome, WaitLimit,
    WorkflowDefConfig, WorkflowDefinition, WorkflowSpecBuilder,
};
use horsies_examples::{
    app_refused, database_url, isolated_profile, main_of, DemoResult, DEFAULT_PROFILE,
};

/// The codes of the pipeline's own errors.
#[derive(Debug, Clone, PartialEq, CustomErrorCode)]
enum PipelineCode {
    /// A dependency did not succeed.
    UpstreamFailed,
}

/// The order that the demo prices.
const ORDER_ID: u32 = 42;

#[task("load_prices")]
async fn load_prices(order_id: u32) -> Result<Vec<u64>, TaskError<Infallible>> {
    Ok(vec![u64::from(order_id), 250, 1_200])
}

/// A node's dependency result arrives as a `TaskOutcome`, bound by the
/// parameter's name.
#[task("sum_prices")]
async fn sum_prices(
    prices: TaskOutcome<Vec<u64>, Infallible>,
) -> Result<u64, TaskError<PipelineCode>> {
    match prices {
        TaskOutcome::Succeeded(prices) => Ok(prices.iter().sum()),
        TaskOutcome::Failed(error) => Err(upstream_failed(&error)),
    }
}

#[task("count_items")]
async fn count_items(
    prices: TaskOutcome<Vec<u64>, Infallible>,
) -> Result<usize, TaskError<PipelineCode>> {
    match prices {
        TaskOutcome::Succeeded(prices) => Ok(prices.len()),
        TaskOutcome::Failed(error) => Err(upstream_failed(&error)),
    }
}

#[task("summarize")]
async fn summarize(
    total: TaskOutcome<u64, PipelineCode>,
    count: TaskOutcome<usize, PipelineCode>,
) -> Result<String, TaskError<PipelineCode>> {
    match (total, count) {
        (TaskOutcome::Succeeded(total), TaskOutcome::Succeeded(count)) => {
            Ok(format!("{count} items, {total} cents"))
        }
        (TaskOutcome::Failed(error), _) | (_, TaskOutcome::Failed(error)) => {
            Err(upstream_failed(&error))
        }
    }
}

fn upstream_failed<E: CustomErrorCode>(error: &TaskError<E>) -> TaskError<PipelineCode> {
    TaskError::custom(PipelineCode::UpstreamFailed, error.to_string())
}

/// The definition: its nodes, and the node whose result is the workflow's.
struct OrderSummary;

impl WorkflowDefinition for OrderSummary {
    type Output = String;
    type Params = ();

    fn name() -> &'static str {
        "order_summary"
    }

    fn definition_key() -> &'static str {
        "examples.order_summary.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        let load =
            builder.task(load_prices::node().set(load_prices::params::order_id(), ORDER_ID)?)?;
        let total =
            builder.task(sum_prices::node().arg_from(sum_prices::params::prices(), load))?;
        let count =
            builder.task(count_items::node().arg_from(count_items::params::prices(), load))?;
        let summary = builder.task(
            summarize::node()
                .arg_from(summarize::params::total(), total)
                .arg_from(summarize::params::count(), count),
        )?;
        Ok(WorkflowDefConfig::new().output(summary))
    }
}

fn build_app() -> Result<Horsies, HorsiesError> {
    let mut config = AppConfig::for_database_url(database_url());
    config.worker_profiles = BTreeMap::from([(DEFAULT_PROFILE.to_owned(), isolated_profile())]);
    let mut app = Horsies::new(config)?;
    let _load_prices = load_prices::register(&mut app)?;
    let _sum_prices = sum_prices::register(&mut app)?;
    let _count_items = count_items::register(&mut app)?;
    let _summarize = summarize::register(&mut app)?;
    // The tasks first: the definition builds its nodes from them.
    let _order_summary = app.register_workflow_definition::<OrderSummary>()?;
    Ok(app)
}

async fn demo(app: Horsies) -> DemoResult {
    let handle = app.start_workflow::<OrderSummary>().await?;
    println!("started workflow {}", handle.workflow_id());
    let limit = WaitLimit::Within(Duration::from_secs(60));
    match handle.get(limit).await? {
        TaskOutcome::Succeeded(summary) => println!("summary: {summary}"),
        TaskOutcome::Failed(error) => println!("the workflow failed: {error}"),
    }
    Ok(())
}

fn main() -> ExitCode {
    match build_app() {
        Ok(app) => main_of(app, demo),
        Err(error) => app_refused(&error),
    }
}
