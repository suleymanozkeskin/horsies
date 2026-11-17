// WorkflowInput: a token per field that serde reads, with its key.
use horsies::WorkflowInput;

#[derive(serde::Serialize, serde::Deserialize, WorkflowInput)]
struct Job {
    name: String,
    r#type: String,
    #[serde(skip)]
    cached: u32,
    #[serde(skip_deserializing)]
    computed: u32,
    #[serde(rename = "retries")]
    retry_count: u32,
}

fn main() {
    let _ = Job::field_name();
    let _ = Job::field_type();
    let _ = Job::field_retry_count();
}
