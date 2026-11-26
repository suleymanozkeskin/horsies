//! The returns workflow: receive the item, inspect it, then restock it or
//! write it off. A damaged item fails the inspection, which pauses the
//! workflow (`OnError::Pause`); the settle node takes failed dependencies,
//! so a resume writes the item off.

use horsies::{HorsiesError, OnError, WorkflowDefinition, WorkflowSpec, WorkflowSpecBuilder};
use serde::{Deserialize, Serialize};

use crate::domain::RestockDecision;
use crate::tasks::returns::{inspect_item, receive_return, restock_or_writeoff};

/// The params of one return.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReturnsParams {
    pub return_id: String,
    pub order_id: String,
    pub sku: String,
    pub quantity: i32,
}

/// The returns workflow. Its result is the restock decision.
pub struct ReturnsReview;

impl WorkflowDefinition for ReturnsReview {
    type Output = RestockDecision;
    type Parameters = ReturnsParams;

    fn name() -> &'static str {
        "returns_review"
    }

    fn definition_key() -> &'static str {
        "acme.returns_review.v1"
    }

    fn on_error() -> OnError {
        OnError::Pause
    }

    fn build_with(
        mut builder: WorkflowSpecBuilder,
        params: ReturnsParams,
    ) -> Result<WorkflowSpec, HorsiesError> {
        builder
            .definition_key(Self::definition_key())
            .on_error(Self::on_error());
        let receive = builder.task(
            receive_return::node()
                .node_id("receive_return")
                .set(
                    receive_return::params::return_id(),
                    params.return_id.clone(),
                )?
                .set(receive_return::params::order_id(), params.order_id)?
                .set(receive_return::params::sku(), params.sku)?
                .set(receive_return::params::quantity(), params.quantity)?,
        )?;
        let inspect = builder.task(
            inspect_item::node()
                .node_id("inspect_item")
                .set(inspect_item::params::return_id(), params.return_id.clone())?
                .waits_for(receive),
        )?;
        let settle = builder.task(
            restock_or_writeoff::node()
                .node_id("restock_or_writeoff")
                .set(restock_or_writeoff::params::return_id(), params.return_id)?
                .arg_from(restock_or_writeoff::params::inspection(), inspect)
                .allow_failed_dependencies(true),
        )?;
        builder.output(settle);
        builder.build()
    }
}

/// The check case of the returns template.
pub fn check_params() -> ReturnsParams {
    ReturnsParams {
        return_id: "RET-CHECK".to_owned(),
        order_id: "ACME-CHECK-0001".to_owned(),
        sku: "ACME-SKU-0001".to_owned(),
        quantity: 1,
    }
}
