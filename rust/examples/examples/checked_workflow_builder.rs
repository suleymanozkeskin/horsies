//! Check-time validation for runtime-built workflow specs.
//!
//! This is the public alpha.5 replacement for the old public
//! `workflow_builder(...).cases(...).register()` story.

use horsies::{
    task, AppConfig, Horsies, TaskError, TaskResult, WorkflowInput, WorkflowSpecBuilder,
};
use horsies_examples::common;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FetchInput {
    source_url: String,
}

#[task("fetch_data")]
async fn fetch_data(_input: FetchInput) -> Result<String, TaskError> {
    Ok("raw".to_owned())
}

/// Input for process_data via args_from — receives TaskResult wrapper.
#[derive(Debug, Clone, Serialize, Deserialize, WorkflowInput)]
struct ProcessInput {
    data: TaskResult<String>,
}

#[task("process_data")]
async fn process_data(input: ProcessInput) -> Result<String, TaskError> {
    let _data = match input.data {
        TaskResult::Ok(v) => v,
        TaskResult::Err(e) => {
            return Err(TaskError::new(
                "UPSTREAM_FAILED",
                format!("upstream failed: {:?}", e.error_code),
            ))
        }
    };
    Ok("processed".to_owned())
}

fn config() -> AppConfig {
    common::app_config(&common::db_url())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut app = Horsies::new(config())?;

    let fetch = fetch_data::register(&mut app)?;
    let process = process_data::register(&mut app)?;

    let mut registration =
        app.check_workflow_builder("build_child_workflow", move |source_url: &String| {
            let mut builder = WorkflowSpecBuilder::new("child_pipeline");
            builder.definition_key("examples.child_pipeline.v1");
            let fetch_ref = builder.task(fetch.node().set_input(FetchInput {
                source_url: source_url.clone(),
            })?);
            let process_ref = builder.task(
                process
                    .node()
                    .waits_for(fetch_ref)
                    .arg_from(ProcessInput::field_data(), fetch_ref),
            );
            builder.output(process_ref);
            builder.build()
        })?;

    registration.cases([
        "https://example.com/source-a.json".to_owned(),
        "https://example.com/source-b.json".to_owned(),
    ]);
    registration.register()?;

    app.check()?;
    println!("checked workflow builder validated representative cases");
    Ok(())
}
