// `arg_from` takes a field token of the node's own input type.
use std::convert::Infallible;

use horsies::{task, TaskError, TaskOutcome, WorkflowSpecBuilder};

#[task("produce_number")]
async fn produce_number() -> Result<i64, TaskError<Infallible>> {
    Ok(1)
}

#[task("consume_number")]
async fn consume_number(value: TaskOutcome<i64, Infallible>) -> Result<(), TaskError<Infallible>> {
    let _ = value;
    Ok(())
}

#[task("other_number")]
async fn other_number(value: TaskOutcome<i64, Infallible>) -> Result<(), TaskError<Infallible>> {
    let _ = value;
    Ok(())
}

fn main() {
    let mut builder = WorkflowSpecBuilder::new("wrong_field_token");
    let produced = builder.task(produce_number::node().unwrap());
    let _ = consume_number::node()
        .unwrap()
        .arg_from(other_number::params::value(), produced);
}
