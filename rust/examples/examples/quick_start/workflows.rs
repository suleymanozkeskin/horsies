//! Workflow definitions for the shipping example.

use horsies::{Horsies, HorsiesError, OnError, WorkflowFunction};

use super::models::*;
use super::tasks::Tasks;

/// Register the order processing workflow using the unified app builder API.
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
pub fn register(
    app: &mut Horsies,
    tasks: &Tasks,
) -> Result<WorkflowFunction<NotificationResult>, HorsiesError> {
    let mut wf = app.workflow::<NotificationResult>("order_processing");
    wf.definition_key("quickstart.order_processing.v1");
    wf.on_error(OnError::Fail);

    // Root task — validates the order
    let validate = wf.task(tasks.validate_order.node().node_id("validate"));

    // Fan-out: three parallel checks, all depend on validate
    let inventory = wf.task(
        tasks
            .check_inventory
            .node()
            .node_id("inventory")
            .waits_for(validate)
            .args_from("order", validate),
    );
    let cost = wf.task(
        tasks
            .calculate_shipping_cost
            .node()
            .node_id("shipping_cost")
            .waits_for(validate)
            .args_from("order", validate),
    );
    let address = wf.task(
        tasks
            .check_address
            .node()
            .node_id("address")
            .waits_for(validate)
            .args_from("order", validate),
    );

    // Fan-in: reserve waits for all three parallel checks
    let reserve = wf.task(
        tasks
            .reserve_inventory
            .node()
            .node_id("reserve")
            .waits_for(inventory)
            .waits_for(cost)
            .waits_for(address)
            .args_from("inventory", inventory)
            .args_from("cost", cost)
            .args_from("address", address),
    );

    // Sequential: shipment then notification
    let shipment = wf.task(
        tasks
            .create_shipment
            .node()
            .node_id("shipment")
            .waits_for(reserve)
            .args_from("reservation", reserve),
    );
    let notify = wf.task(
        tasks
            .send_notification
            .node()
            .node_id("notify")
            .waits_for(shipment)
            .args_from("shipment", shipment),
    );

    wf.output(notify);
    wf.build()
}
