// The workflow DSL over the generated helpers: a node per task, a result
// passed with `arg_from` (the receiving type `TaskOutcome`), the workflow
// context, a builder of the app, and a definition with its sub-workflow
// node.
use std::convert::Infallible;

use horsies::{
    task, AppConfig, Horsies, HorsiesError, SubWorkflowParams, TaskError, TaskOutcome,
    WorkflowContext, WorkflowDefConfig, WorkflowDefinition, WorkflowSpecBuilder,
};

#[derive(serde::Serialize, serde::Deserialize)]
struct Order {
    order_id: u32,
}

#[task("produce")]
async fn produce(order: Order) -> Result<f64, TaskError<Infallible>> {
    Ok(f64::from(order.order_id))
}

#[task("consume")]
async fn consume(total: TaskOutcome<f64, Infallible>) -> Result<String, TaskError<Infallible>> {
    match total {
        TaskOutcome::Succeeded(value) => Ok(value.to_string()),
        TaskOutcome::Failed(_error) => Ok(String::new()),
    }
}

#[task("summarize", workflow_context)]
async fn summarize(workflow_context: WorkflowContext) -> Result<u32, TaskError<Infallible>> {
    Ok(u32::try_from(workflow_context.task_index()).unwrap_or(0))
}

struct Pipeline;

impl WorkflowDefinition for Pipeline {
    type Output = String;
    type Params = ();

    fn name() -> &'static str {
        "pipeline"
    }

    fn definition_key() -> &'static str {
        "tests.pipeline.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        let produced =
            builder.task(produce::node().set(produce::fields::order(), Order { order_id: 7 })?)?;
        let consumed =
            builder.task(consume::node().arg_from(consume::fields::total(), produced))?;
        let _summary = builder.task(
            summarize::node()
                .waits_for(consumed)
                .workflow_context_from([consumed]),
        )?;
        Ok(WorkflowDefConfig::new().output(consumed))
    }
}

fn main() {
    let Ok(mut app) = Horsies::new(AppConfig::for_database_url(
        "postgresql://localhost/horsies",
    )) else {
        return;
    };
    let _produce = produce::register(&mut app);
    let _consume = consume::register(&mut app);
    let _summarize = summarize::register(&mut app);
    let _spec = Pipeline::build(app.workflow_builder(Pipeline::name()));
    let _function = app.register_workflow_definition::<Pipeline>();
    let _node = Pipeline::node(SubWorkflowParams::NotGiven);
}
