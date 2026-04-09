use horsies::{task, TaskError, TaskResult, TypedNodeRef};

#[task("consume_number")]
async fn consume_number(value: TaskResult<i64>) -> Result<(), TaskError> {
    let _ = value;
    Ok(())
}

#[task("other_number")]
async fn other_number(value: TaskResult<i64>) -> Result<(), TaskError> {
    let _ = value;
    Ok(())
}

fn main() {
    let dep = TypedNodeRef::<i64>::new(0);
    let _ = consume_number::node()
        .unwrap()
        .arg_from(other_number::params::value(), dep);
}
