// A custom code with a reserved name that is not a task code.
use horsies::CustomErrorCode;

#[derive(Debug, Clone, PartialEq, CustomErrorCode)]
enum FlowCode {
    WorkflowStopped,
}

fn main() {}
