// A custom code with the name of a built-in code: `TaskTimeout` is TASK_TIMEOUT.
use horsies::CustomErrorCode;

#[derive(Debug, Clone, PartialEq, CustomErrorCode)]
enum ChargeCode {
    CardDeclined,
    TaskTimeout,
}

fn main() {}
