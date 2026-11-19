//! `GET /metrics` of the monitoring handler: the fleet collector of the
//! handler's broker.
//!
//! The first request starts the reads of the fleet state on the runtime
//! that serves it; its answer holds no fleet series yet. From then on the
//! state is read at most once per
//! [`FLEET_COLLECT_INTERVAL`](crate::monitoring::fleet::FLEET_COLLECT_INTERVAL),
//! and a request renders the last read. The reads stop when the handler is
//! dropped.

use std::sync::{Arc, Once};

use tokio_util::sync::{CancellationToken, DropGuard};

use crate::broker::PostgresBroker;
use crate::monitoring::fleet::{collect_fleet_state, FleetCollector, FleetLabels};

/// The fleet collector of one handler.
pub(crate) struct WebFleetMetrics {
    collector: Arc<FleetCollector>,
    broker: Arc<PostgresBroker>,
    started: Once,
    /// The stop of the reads: a clone goes to the reads.
    reads_stop: CancellationToken,
    /// Cancels `reads_stop` when the handler's state is dropped.
    _stop_on_drop: DropGuard,
}

impl WebFleetMetrics {
    /// The collector of `broker`, with the queue and schedule names of
    /// `labels`. Starts nothing.
    pub(crate) fn new(broker: Arc<PostgresBroker>, labels: FleetLabels) -> Self {
        let stop = CancellationToken::new();
        Self {
            collector: Arc::new(FleetCollector::new(labels)),
            broker,
            started: Once::new(),
            reads_stop: stop.clone(),
            _stop_on_drop: stop.drop_guard(),
        }
    }

    /// The Prometheus text of the last read. The first call starts the
    /// reads with `tokio::spawn`, so it runs inside a tokio runtime (the
    /// router's); it reads nothing itself.
    pub(crate) fn render(&self) -> String {
        self.started.call_once(|| self.start_reads());
        self.collector.render()
    }

    fn start_reads(&self) {
        let collector = Arc::clone(&self.collector);
        let broker = Arc::clone(&self.broker);
        let stop = self.reads_stop.clone();
        let _reads = tokio::spawn(async move {
            collect_fleet_state(&collector, &broker, &stop).await;
        });
    }
}
