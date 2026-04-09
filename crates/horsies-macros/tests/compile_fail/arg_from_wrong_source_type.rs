use horsies::{task, TaskError, TaskResult, TypedNodeRef};

#[task("consume_number")]
async fn consume_number(value: TaskResult<i64>) -> Result<(), TaskError> {
    let _ = value;
    Ok(())
}

fn main() {
    let dep = TypedNodeRef::<String>::new(0);
    let _ = consume_number::node()
        .unwrap()
        .arg_from(consume_number::params::value(), dep);
}
