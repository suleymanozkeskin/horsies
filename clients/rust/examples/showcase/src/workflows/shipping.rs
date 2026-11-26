//! The shipping workflow: book the courier, print the label, seed the
//! tracking code. The order workflow runs it as a sub-workflow.

use horsies::{HorsiesError, OnError, WorkflowDefinition, WorkflowSpec, WorkflowSpecBuilder};
use serde::{Deserialize, Serialize};

use crate::domain::TrackingSeed;
use crate::tasks::shipping::{book_courier, print_label, tracking_seed};

/// The params of one shipment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShippingParams {
    pub order_id: String,
    pub courier: String,
    pub express: bool,
}

/// The shipping workflow. Its result is the tracking code.
pub struct Shipping;

impl WorkflowDefinition for Shipping {
    type Output = TrackingSeed;
    type Params = ShippingParams;

    fn name() -> &'static str {
        "shipping"
    }

    fn definition_key() -> &'static str {
        "acme.shipping.v1"
    }

    fn build_with(
        mut builder: WorkflowSpecBuilder,
        params: ShippingParams,
    ) -> Result<WorkflowSpec, HorsiesError> {
        builder
            .definition_key(Self::definition_key())
            .on_error(OnError::Fail);
        let book = builder.task(
            book_courier::node()
                .node_id("book_courier")
                .set(book_courier::fields::order_id(), params.order_id.clone())?
                .set(book_courier::fields::courier(), params.courier)?
                .set(book_courier::fields::express(), params.express)?,
        )?;
        let label = builder.task(
            print_label::node()
                .node_id("print_label")
                .set(print_label::fields::order_id(), params.order_id.clone())?
                .arg_from(print_label::fields::booking(), book),
        )?;
        let tracking = builder.task(
            tracking_seed::node()
                .node_id("tracking_seed")
                .set(tracking_seed::fields::order_id(), params.order_id)?
                .arg_from(tracking_seed::fields::label(), label),
        )?;
        builder.output(tracking);
        builder.build()
    }
}

/// The check case of the shipping template.
pub fn check_params() -> ShippingParams {
    ShippingParams {
        order_id: "CHECK-ORDER".to_owned(),
        courier: "fleetline".to_owned(),
        express: false,
    }
}
