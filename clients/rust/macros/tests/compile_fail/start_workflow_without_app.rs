// A workflow start names the app that it goes through: the crate has no
// process-wide start of a definition.
use horsies::{OutputlessResults, WorkflowDefinition};

struct Nightly;

impl WorkflowDefinition for Nightly {
    type Output = OutputlessResults;
    type Params = ();

    fn name() -> &'static str {
        "nightly"
    }

    fn definition_key() -> &'static str {
        "tests.nightly.v1"
    }
}

fn main() {
    let _future = horsies::start_workflow::<Nightly>();
}
