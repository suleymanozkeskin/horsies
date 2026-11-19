// A flattened field has no key of its own.
use horsies::WorkflowInput;

#[derive(serde::Serialize, serde::Deserialize)]
struct Inner {
    count: u32,
}

#[derive(serde::Serialize, serde::Deserialize, WorkflowInput)]
struct Outer {
    name: String,
    #[serde(flatten)]
    inner: Inner,
}

fn main() {}
