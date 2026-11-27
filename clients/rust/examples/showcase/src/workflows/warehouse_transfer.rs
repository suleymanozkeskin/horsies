//! The warehouse transfer: pick the source warehouse, then put the units
//! back on the shelf at the destination.

use horsies::{HorsiesError, WorkflowDefConfig, WorkflowDefinition, WorkflowSpecBuilder};

use crate::domain::StockRelease;
use crate::tasks::inventory::release_stock;
use crate::tasks::orders::allocate_warehouse;

/// The SKU that the definition transfers.
pub const SKU: &str = "ACME-SKU-0001";
/// The units that the definition transfers.
pub const QUANTITY: i32 = 5;

/// The warehouse transfer workflow. Its result is the release.
pub struct WarehouseTransfer;

impl WorkflowDefinition for WarehouseTransfer {
    type Output = StockRelease;
    type Params = ();

    fn name() -> &'static str {
        "warehouse_transfer"
    }

    fn definition_key() -> &'static str {
        "acme.warehouse_transfer.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        let allocate = builder.task(
            allocate_warehouse::node()
                .node_id("allocate_warehouse")
                .set(allocate_warehouse::fields::sku(), SKU.to_owned())?
                .set(allocate_warehouse::fields::quantity(), QUANTITY)?,
        )?;
        let release = builder.task(
            release_stock::node()
                .node_id("release_stock")
                .set(release_stock::fields::sku(), SKU.to_owned())?
                .set(release_stock::fields::quantity(), QUANTITY)?
                .waits_for(allocate),
        )?;
        Ok(WorkflowDefConfig::new().output(release))
    }
}
