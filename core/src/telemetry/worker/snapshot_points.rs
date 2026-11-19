//! The points of the worker snapshot: the gauges of the seats, of the down
//! slots, of the seats in use and of the memory of the worker parent.
//!
//! The snapshot loop (`crate::worker::worker_state::write_snapshot`) reads
//! the slot board, the process and the rows of the worker once per
//! snapshot, writes the values to its row of `horsies_worker_states`, and
//! gives the same values to these points. A gauge and its column have one
//! source, and a gauge changes once per snapshot interval
//! (`recovery.worker_state_snapshot_interval_ms`), not at a scrape.
//!
//! | Point | Metrics | Columns of the same snapshot |
//! |---|---|---|
//! | [`WorkerTelemetry::slots_read`] | `horsies.worker.seats{execution}`, `horsies.worker.slots.down{kind}` | `class_capacity`, `slots_down` |
//! | [`WorkerTelemetry::parent_rss_read`] | `horsies.worker.memory.rss{process="parent"}` | `memory_usage_mb` |
//! | [`WorkerTelemetry::held_rows_read`] | `horsies.worker.seats.in_use{execution}` | `class_capacity`; one statement with `tasks_running`, `tasks_claimed` |
//!
//! - The seats, the down slots and the seats in use have one series per
//!   class, 0 included.
//! - A count of the held rows that failed leaves
//!   `horsies.worker.seats.in_use` with no series until a snapshot counts
//!   again: never the value of an earlier snapshot.
//! - An RSS that the process table does not give leaves the last `parent`
//!   value: the metric also holds the `handler_child` series, which the
//!   handler child's stats frames set, so its series are set one by one.
//! - `process="children"` has no series: the snapshot does not read the RSS
//!   of the task children (its `children_memory_mb` column is NULL).

use super::{TelemetryOn, WorkerTelemetry};
use crate::core::task::Execution;
use crate::telemetry::labels::MemoryProcess;
use crate::worker::worker_state::{ClassCounts, ParentRss, SeatsInUse};

impl WorkerTelemetry {
    /// A snapshot read the slots: `seats` are the seats in service of each
    /// budget class, `down` the down slots of each kind.
    pub(crate) fn slots_read(&self, seats: ClassCounts, down: ClassCounts) {
        match self {
            Self::Off => {}
            Self::On(on) => on.slots_read(seats, down),
        }
    }

    /// A snapshot read the RSS of the worker parent.
    pub(crate) fn parent_rss_read(&self, rss: ParentRss) {
        match self {
            Self::Off => {}
            Self::On(on) => on.parent_rss_read(rss),
        }
    }

    /// A snapshot counted the rows that the worker holds, or failed to.
    pub(crate) fn held_rows_read(&self, in_use: SeatsInUse) {
        match self {
            Self::Off => {}
            Self::On(on) => on.held_rows_read(in_use),
        }
    }
}

/// One series per class of `counts`, 0 included.
fn class_series(counts: ClassCounts) -> impl Iterator<Item = ((Execution,), f64)> {
    Execution::ALL
        .into_iter()
        .map(move |class| ((class,), f64::from(counts.of(class))))
}

impl TelemetryOn {
    fn slots_read(&self, seats: ClassCounts, down: ClassCounts) {
        let _seats = self.metrics.seats.replace_all(class_series(seats));
        let _down = self.metrics.slots_down.replace_all(class_series(down));
    }

    fn parent_rss_read(&self, rss: ParentRss) {
        match rss {
            // A gauge holds an `f64`: exact up to 2^53 bytes.
            ParentRss::Bytes(bytes) => {
                let _recorded = self
                    .metrics
                    .memory_rss
                    .set((MemoryProcess::Parent,), bytes as f64);
            }
            ParentRss::Unreadable => {}
        }
    }

    fn held_rows_read(&self, in_use: SeatsInUse) {
        let _replaced = match in_use {
            SeatsInUse::Counted(counts) => {
                self.metrics.seats_in_use.replace_all(class_series(counts))
            }
            SeatsInUse::NotCounted => self.metrics.seats_in_use.replace_all(std::iter::empty()),
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::catalog::MetricId;
    use crate::telemetry::registry::SampleValue;
    use crate::test_support::telemetry::WatchedTelemetry;

    fn counts(isolated: u32, shared_async: u32, shared_thread: u32) -> ClassCounts {
        ClassCounts {
            isolated,
            shared_async,
            shared_thread,
        }
    }

    /// The series of `metric` as (label texts, gauge value), sorted by
    /// label texts.
    fn gauges(watched: &WatchedTelemetry, metric: MetricId) -> Vec<(Vec<String>, f64)> {
        watched
            .series(metric)
            .into_iter()
            .map(|(labels, value)| match value {
                SampleValue::Gauge(value) => (labels, value),
                other => panic!("{metric:?} is a gauge, got {other:?}"),
            })
            .collect()
    }

    fn by_class(isolated: f64, shared_async: f64, shared_thread: f64) -> Vec<(Vec<String>, f64)> {
        vec![
            (vec!["ISOLATED".to_owned()], isolated),
            (vec!["SHARED_ASYNC".to_owned()], shared_async),
            (vec!["SHARED_THREAD".to_owned()], shared_thread),
        ]
    }

    #[test]
    fn a_slot_read_sets_the_seats_and_the_down_slots_of_each_class() {
        let watched = WatchedTelemetry::new();

        watched
            .telemetry
            .slots_read(counts(2, 40, 0), counts(1, 0, 0));
        assert_eq!(gauges(&watched, MetricId::Seats), by_class(2.0, 40.0, 0.0));
        assert_eq!(
            gauges(&watched, MetricId::SlotsDown),
            by_class(1.0, 0.0, 0.0)
        );

        // The next snapshot replaces each value.
        watched
            .telemetry
            .slots_read(counts(3, 40, 0), counts(0, 0, 0));
        assert_eq!(gauges(&watched, MetricId::Seats), by_class(3.0, 40.0, 0.0));
        assert_eq!(
            gauges(&watched, MetricId::SlotsDown),
            by_class(0.0, 0.0, 0.0)
        );
    }

    #[test]
    fn a_count_sets_the_seats_in_use_and_a_failed_count_leaves_no_series() {
        let watched = WatchedTelemetry::new();

        watched
            .telemetry
            .held_rows_read(SeatsInUse::Counted(counts(2, 7, 0)));
        assert_eq!(
            gauges(&watched, MetricId::SeatsInUse),
            by_class(2.0, 7.0, 0.0)
        );

        watched.telemetry.held_rows_read(SeatsInUse::NotCounted);
        assert_eq!(gauges(&watched, MetricId::SeatsInUse), Vec::new());
    }

    #[test]
    fn a_parent_rss_sets_the_parent_series_and_an_unreadable_one_keeps_it() {
        let watched = WatchedTelemetry::new();

        watched
            .telemetry
            .parent_rss_read(ParentRss::Bytes(52_428_800));
        assert_eq!(
            gauges(&watched, MetricId::MemoryRss),
            vec![(vec!["parent".to_owned()], 52_428_800.0)]
        );

        watched.telemetry.parent_rss_read(ParentRss::Unreadable);
        assert_eq!(
            gauges(&watched, MetricId::MemoryRss),
            vec![(vec!["parent".to_owned()], 52_428_800.0)]
        );
    }

    /// The handler child sets its own series of the memory metric; the
    /// parent's series leaves it.
    #[test]
    fn the_parent_rss_leaves_the_series_of_the_handler_child() {
        let watched = WatchedTelemetry::new();
        let rss = watched
            .registry
            .gauge::<(MemoryProcess,)>(MetricId::MemoryRss)
            .expect("the worker registry holds the memory gauge");
        let _recorded = rss.set((MemoryProcess::HandlerChild,), 1_048_576.0);

        watched
            .telemetry
            .parent_rss_read(ParentRss::Bytes(2_097_152));

        assert_eq!(
            gauges(&watched, MetricId::MemoryRss),
            vec![
                (vec!["handler_child".to_owned()], 1_048_576.0),
                (vec!["parent".to_owned()], 2_097_152.0),
            ]
        );
    }
}
