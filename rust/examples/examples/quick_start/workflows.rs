//! Workflow definitions for the shipping example.

use horsies::{
    Horsies, HorsiesError, OnError, WorkflowDefinition, WorkflowSpec, WorkflowSpecBuilder,
    WorkflowTemplate,
};

use super::models::*;
use super::tasks::{
    calculate_shipping_cost, check_address, check_inventory, create_shipment, reserve_inventory,
    send_notification, validate_order,
};

/// Reusable order-processing workflow definition.
///
/// The workflow DAG:
///
/// ```text
///   validate_order
///       /    |    \
///      v     v     v
///  inventory cost  address
///       \    |    /
///        v   v   v
///      reserve_inventory
///            |
///            v
///      create_shipment
///            |
///            v
///      send_notification
/// ```
pub struct OrderProcessingWorkflow;

impl WorkflowDefinition for OrderProcessingWorkflow {
    type Output = NotificationResult;
    type Params = Order;

    fn name() -> &'static str {
        "order_processing"
    }

    fn definition_key() -> &'static str {
        "quickstart.order_processing.v1"
    }

    fn on_error() -> OnError {
        OnError::Fail
    }

    fn build_with(order: Order) -> Result<WorkflowSpec, HorsiesError> {
        let mut builder = WorkflowSpecBuilder::new(Self::name());
        builder.definition_key(Self::definition_key());
        builder.on_error(Self::on_error());

        // Root task — validates the order
        let validate = builder.task(validate_order::node_with(order)?.node_id("validate"));

        // Fan-out: three parallel checks, all depend on validate
        let inventory = builder.task(
            check_inventory::node()?
                .node_id("inventory")
                .waits_for(validate)
                .args_from("order", validate),
        );
        let cost = builder.task(
            calculate_shipping_cost::node()?
                .node_id("shipping_cost")
                .waits_for(validate)
                .args_from("order", validate),
        );
        let address = builder.task(
            check_address::node()?
                .node_id("address")
                .waits_for(validate)
                .args_from("order", validate),
        );

        // Fan-in: reserve waits for all three parallel checks
        let reserve = builder.task(
            reserve_inventory::node()?
                .node_id("reserve")
                .waits_for(inventory)
                .waits_for(cost)
                .waits_for(address)
                .args_from("inventory", inventory)
                .args_from("cost", cost)
                .args_from("address", address),
        );

        // Sequential: shipment then notification
        let shipment = builder.task(
            create_shipment::node()?
                .node_id("shipment")
                .waits_for(reserve)
                .args_from("reservation", reserve),
        );
        let notify = builder.task(
            send_notification::node()?
                .node_id("notify")
                .waits_for(shipment)
                .args_from("shipment", shipment),
        );

        builder.output(notify);
        builder.build()
    }
}

/// Register the order-processing workflow template on the app.
pub fn register(
    app: &mut Horsies,
) -> Result<WorkflowTemplate<Order, NotificationResult>, HorsiesError> {
    Ok(app.workflow_template::<OrderProcessingWorkflow>())
}
