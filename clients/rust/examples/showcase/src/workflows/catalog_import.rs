//! The catalog import: one node per chunk and no output node. Its result
//! is the outcome of every chunk by node id (`OutputlessResults`).

use horsies::{
    HorsiesError, OutputlessResults, WorkflowDefinition, WorkflowDefinitionConfig,
    WorkflowSpecBuilder,
};

use crate::tasks::analytics::catalog_import_chunk;
use crate::tuning;

/// The import that the definition imports.
pub const IMPORT_ID: &str = "CHECK-IMPORT";

/// The catalog import workflow.
pub struct CatalogImport;

impl WorkflowDefinition for CatalogImport {
    type Output = OutputlessResults;
    type Parameters = ();

    fn name() -> &'static str {
        "catalog_import"
    }

    fn definition_key() -> &'static str {
        "acme.catalog_import.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefinitionConfig, HorsiesError> {
        let chunks = i32::try_from(tuning::CATALOG_IMPORT_CHUNKS)
            .map_err(|_| HorsiesError::new("CATALOG_IMPORT_CHUNKS is above i32::MAX"))?;
        for chunk_index in 0..chunks {
            let _chunk = builder.task(
                catalog_import_chunk::node()
                    .node_id(format!("chunk_{chunk_index:02}"))
                    .set(
                        catalog_import_chunk::fields::import_id(),
                        IMPORT_ID.to_owned(),
                    )?
                    .set(catalog_import_chunk::fields::chunk_index(), chunk_index)?,
            )?;
        }
        Ok(WorkflowDefinitionConfig::new())
    }
}
