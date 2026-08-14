use horsies::{HorsiesError, OnError, WorkflowSpec, WorkflowSpecBuilder};
use serde_json::json;

use super::{finish, WorkflowTasks};

pub fn build(
    tasks: &WorkflowTasks,
    import_id: &str,
    chunks: usize,
) -> Result<WorkflowSpec, HorsiesError> {
    let mut builder = WorkflowSpecBuilder::new("catalog_import");
    builder
        .definition_key("acme.catalog_import.v1")
        .on_error(OnError::Fail);
    for index in 0..chunks {
        builder.task(tasks.node(
            "catalog_import_chunk",
            &format!("chunk_{index:02}"),
            json!({"import_id": import_id, "chunk_index": index}),
        )?);
    }
    finish(builder, None, &[], &[], None)
}
