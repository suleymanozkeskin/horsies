// A user code with the name of a built-in code: `TaskTimeout` is TASK_TIMEOUT.
use horsies::UserErrorCode;

#[derive(Debug, Clone, PartialEq, UserErrorCode)]
enum ChargeCode {
    CardDeclined,
    TaskTimeout,
}

fn main() {}
