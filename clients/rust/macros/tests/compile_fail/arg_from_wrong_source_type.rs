// `arg_from` takes a source whose output type is the field's.
use std::convert::Infallible;

use horsies::{task, AppConfig, Horsies, TaskError, TaskOutcome};

#[task("produce_text")]
async fn produce_text() -> Result<String, TaskError<Infallible>> {
    Ok(String::new())
}

#[task("consume_number")]
async fn consume_number(value: TaskOutcome<i64, Infallible>) -> Result<(), TaskError<Infallible>> {
    let _ = value;
    Ok(())
}

fn main() {
    let mut builder = app().workflow_builder("wrong_source_type");
    let produced = builder.task(produce_text::node()).unwrap();
    let _ = consume_number::node()
        .node_id("consume")
        .arg_from(consume_number::fields::value(), produced);
}

fn app() -> Horsies {
    Horsies::new(AppConfig::from_database_url(
        "postgresql://localhost/horsies",
    ))
    .unwrap()
}
