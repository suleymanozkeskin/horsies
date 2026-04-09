//! Compile-checked mirror of the README API story.
//!
//! This example exists so the README snippets have a concrete compiled
//! counterpart in the examples crate.

use std::time::Duration;

use horsies::{
    task, AppConfig, Horsies, HorsiesError, PostgresConfig, QueueMode, TaskError, TaskResult,
    TaskRuntime, WorkflowDefConfig, WorkflowDefinition, WorkflowInput, WorkflowSpec,
    WorkflowSpecBuilder,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AddNumbersInput {
    a: i32,
    b: i32,
}

#[task("add_numbers")]
async fn add_numbers(args: AddNumbersInput) -> Result<i32, TaskError> {
    Ok(args.a + args.b)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FetchDataInput {
    source: String,
}

#[task("fetch_data")]
async fn fetch_data(_input: FetchDataInput) -> Result<String, TaskError> {
    Ok("raw".to_owned())
}

/// Input for process_data via args_from — receives TaskResult wrapper.
#[derive(Debug, Clone, Serialize, Deserialize, WorkflowInput)]
struct ProcessDataInput {
    data: TaskResult<String>,
}

#[task("process_data")]
async fn process_data(input: ProcessDataInput) -> Result<String, TaskError> {
    let _data = match input.data {
        TaskResult::Ok(v) => v,
        TaskResult::Err(e) => {
            return Err(TaskError::user(
                "UPSTREAM_FAILED",
                format!("upstream failed: {:?}", e.error_code),
            ))
        }
    };
    Ok("processed".to_owned())
}

/// Input for save_result via args_from — receives TaskResult wrapper.
#[derive(Debug, Clone, Serialize, Deserialize, WorkflowInput)]
struct SaveResultInput {
    result: TaskResult<String>,
}

#[task("save_result")]
async fn save_result(input: SaveResultInput) -> Result<String, TaskError> {
    let _result = match input.result {
        TaskResult::Ok(v) => v,
        TaskResult::Err(e) => {
            return Err(TaskError::user(
                "UPSTREAM_FAILED",
                format!("upstream failed: {:?}", e.error_code),
            ))
        }
    };
    Ok("saved".to_owned())
}

#[task("notify_user")]
async fn notify_user(data: TaskResult<String>, urgent: bool) -> Result<(), TaskError> {
    let _data = match data {
        TaskResult::Ok(v) => v,
        TaskResult::Err(e) => {
            return Err(TaskError::user(
                "UPSTREAM_FAILED",
                format!("upstream failed: {:?}", e.error_code),
            ))
        }
    };
    let _urgent = urgent;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChildInput {
    source_url: String,
}

fn build_child_spec(input: &ChildInput) -> Result<WorkflowSpec, HorsiesError> {
    let mut builder = WorkflowSpecBuilder::new("child_pipeline");
    builder.definition_key("examples.child_pipeline.dynamic.v1");
    let fetch = builder.task(
        fetch_data::node()?
            .set_input(FetchDataInput {
                source: input.source_url.clone(),
            })?
            .node_id("fetch"),
    );
    let process = builder.task(
        process_data::node()?
            .node_id("process")
            .waits_for(fetch)
            .arg_from(ProcessDataInput::field_data(), fetch),
    );
    builder.output(process);
    builder.build()
}

fn build_mixed_binding_spec(input: &ChildInput) -> Result<WorkflowSpec, HorsiesError> {
    let mut builder = WorkflowSpecBuilder::new("notify_pipeline");
    builder.definition_key("examples.notify_pipeline.v1");
    let fetch = builder.task(
        fetch_data::node()?
            .set_input(FetchDataInput {
                source: input.source_url.clone(),
            })?
            .node_id("fetch"),
    );
    let process = builder.task(
        process_data::node()?
            .node_id("process")
            .waits_for(fetch)
            .arg_from(ProcessDataInput::field_data(), fetch),
    );
    let notify = builder.task(
        notify_user::node()?
            .node_id("notify")
            .waits_for(process)
            .arg_from(notify_user::params::data(), process)
            .set(notify_user::params::urgent(), true)?,
    );
    builder.output(notify);
    builder.build()
}

struct ETLPipeline;

impl WorkflowDefinition for ETLPipeline {
    type Output = String;
    type Params = ();

    fn name() -> &'static str {
        "etl_pipeline"
    }

    fn definition_key() -> &'static str {
        "examples.etl_pipeline.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        let fetch = builder.task(
            fetch_data::node()?
                .set_input(FetchDataInput {
                    source: "default".to_owned(),
                })?
                .node_id("fetch"),
        );
        let process = builder.task(
            process_data::node()?
                .node_id("process")
                .waits_for(fetch)
                .arg_from(ProcessDataInput::field_data(), fetch),
        );
        let save = builder.task(
            save_result::node()?
                .node_id("save")
                .waits_for(process)
                .arg_from(SaveResultInput::field_result(), process),
        );
        Ok(WorkflowDefConfig::new().output(save))
    }
}

struct ChildPipeline;

impl WorkflowDefinition for ChildPipeline {
    type Output = String;
    type Params = String;

    fn name() -> &'static str {
        "child_pipeline"
    }

    fn definition_key() -> &'static str {
        "examples.child_pipeline.v1"
    }

    fn build_with(source_url: Self::Params) -> Result<WorkflowSpec, HorsiesError> {
        let mut builder = WorkflowSpecBuilder::new("child_pipeline");
        builder.definition_key("examples.child_pipeline.v1");
        let fetch = builder.task(
            fetch_data::node()?
                .set_input(FetchDataInput { source: source_url })?
                .node_id("fetch"),
        );
        let process = builder.task(
            process_data::node()?
                .node_id("process")
                .waits_for(fetch)
                .arg_from(ProcessDataInput::field_data(), fetch),
        );
        builder.output(process);
        builder.build()
    }
}

#[task("build_child_workflow")]
async fn build_child_workflow(rt: TaskRuntime, input: ChildInput) -> Result<(), TaskError> {
    if let Some(spec) = Some(
        build_child_spec(&input)
            .map_err(|err| TaskError::user("WF_BUILD_FAILED", err.to_string()))?,
    ) {
        match rt.start::<String>(spec).await {
            Ok(handle) => {
                tracing::info!(workflow_id = %handle.workflow_id(), "started child workflow");
            }
            Err(err) => {
                tracing::warn!(error = %err.message, "failed to start child workflow");
            }
        }
    }
    Ok(())
}

#[task("enqueue_add_numbers")]
async fn enqueue_add_numbers(rt: TaskRuntime) -> Result<(), TaskError> {
    match add_numbers::send(AddNumbersInput { a: 2, b: 3 }).await {
        Ok(handle) => {
            tracing::info!(task_id = %handle.task_id(), "sent add_numbers");
        }
        Err(err) => {
            tracing::warn!(error = %err.message, "failed to send add_numbers");
        }
    }

    match add_numbers::schedule(Duration::from_secs(30), AddNumbersInput { a: 5, b: 8 }).await {
        Ok(handle) => {
            tracing::info!(task_id = %handle.task_id(), "scheduled add_numbers");
        }
        Err(err) => {
            tracing::warn!(error = %err.message, "failed to schedule add_numbers");
        }
    }

    let add_numbers_task = add_numbers::handle(&rt)?;
    match add_numbers_task
        .send(AddNumbersInput { a: 13, b: 21 })
        .await
    {
        Ok(handle) => {
            tracing::info!(task_id = %handle.task_id(), "sent add_numbers via handle");
        }
        Err(err) => {
            tracing::warn!(error = %err.message, "failed to send add_numbers via handle");
        }
    }

    Ok(())
}

struct AppSettings {
    bundesland: String,
}

#[task("use_settings")]
async fn use_settings(rt: TaskRuntime) -> Result<(), TaskError> {
    let settings = rt.state::<AppSettings>()?;
    tracing::info!(bundesland = %settings.bundesland);
    Ok(())
}

fn config() -> AppConfig {
    AppConfig {
        queue_mode: QueueMode::Default,
        custom_queues: None,
        broker: PostgresConfig {
            database_url: std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgresql://localhost/horsies_example".to_owned()),
            pool_pre_ping: true,
            pool_size: 5,
            max_overflow: 5,
            pool_timeout: 30,
            pool_recycle: 1800,
            echo: false,
        },
        resend_on_transient_err: false,
        cluster_wide_cap: None,
        prefetch_buffer: 0,
        claim_lease_ms: None,
        max_claim_renew_age_ms: 180_000,
        recovery: horsies::RecoveryConfig::default(),
        resilience: horsies::WorkerResilienceConfig::default(),
        schedule: None,
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut app = Horsies::new(config())?;

    add_numbers::register(&mut app)?;
    fetch_data::register(&mut app)?;
    process_data::register(&mut app)?;
    save_result::register(&mut app)?;
    notify_user::register(&mut app)?;
    build_child_workflow::register(&mut app)?;
    enqueue_add_numbers::register(&mut app)?;
    use_settings::register(&mut app)?;

    let _workflow = app.register_workflow_definition::<ETLPipeline>()?;
    let _child = app.workflow_template::<ChildPipeline>();
    let _mixed = build_mixed_binding_spec(&ChildInput {
        source_url: "https://example.com/data.json".to_owned(),
    })?;

    let mut checked = app
        .check_workflow_builder("build_child_workflow_cases", |source_url: &String| {
            ChildPipeline::build_with(source_url.clone())
        })?;
    checked.cases([
        "https://example.com/source-a.json".to_owned(),
        "https://example.com/source-b.json".to_owned(),
    ]);
    checked.register()?;

    app.provide(AppSettings {
        bundesland: "berlin".to_owned(),
    })?;

    app.check()?;
    println!("README reference example compiled and validated");
    Ok(())
}
