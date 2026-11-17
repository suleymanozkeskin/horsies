// `arg_from` takes a source whose output type is the field's.
use std::convert::Infallible;

use horsies::{task, TaskError, TaskOutcome, WorkflowSpecBuilder};

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
    let mut builder = WorkflowSpecBuilder::new("wrong_source_type");
    let produced = builder.task(produce_text::node().unwrap());
    let _ = consume_number::node()
        .unwrap()
        .arg_from(consume_number::params::value(), produced);
}
