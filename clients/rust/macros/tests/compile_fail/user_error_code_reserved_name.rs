// A user code with a reserved name that is not a task code.
use horsies::UserErrorCode;

#[derive(Debug, Clone, PartialEq, UserErrorCode)]
enum FlowCode {
    WorkflowStopped,
}

fn main() {}
