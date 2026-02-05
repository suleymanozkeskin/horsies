//! PostgreSQL NOTIFY/LISTEN with debouncing for real-time updates.

use sqlx::postgres::PgListener;
use sqlx::PgPool;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc::Sender, Notify};

use crate::action::ListenerState;
use crate::tui::{Event, NotifyBatch};

/// Channels to listen for horsies state changes.
const CHANNELS: &[&str] = &[
    "horsies_task_status",
    "horsies_workflow_status",
    "horsies_worker_state",
];

/// Debounce window in milliseconds.
const DEBOUNCE_MS: u64 = 150;

/// Reconnection delay after connection failure.
const RECONNECT_DELAY_SECS: u64 = 5;

/// Handle to the background NOTIFY listener task.
pub struct NotifyListenerHandle {
    stop_flag: Arc<AtomicBool>,
    stop_notify: Arc<Notify>,
}

impl NotifyListenerHandle {
    /// Spawn a background listener task.
    ///
    /// Returns `None` if pool is `None` (demo mode).
    pub fn spawn(pool: Option<PgPool>, event_tx: Sender<Event>) -> Option<Self> {
        let pool = pool?;
        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_notify = Arc::new(Notify::new());
        let flag = Arc::clone(&stop_flag);
        let notify = Arc::clone(&stop_notify);

        tokio::spawn(async move {
            listener_loop(pool, event_tx, flag, notify).await;
        });

        Some(Self {
            stop_flag,
            stop_notify,
        })
    }

    /// Signal the listener to stop.
    pub fn stop(&self) {
        self.stop_flag.store(true, Ordering::Relaxed);
        self.stop_notify.notify_waiters();
    }
}

impl Drop for NotifyListenerHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Main listener loop with reconnection logic.
async fn listener_loop(
    pool: PgPool,
    tx: Sender<Event>,
    stop: Arc<AtomicBool>,
    stop_notify: Arc<Notify>,
) {
    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }

        match connect_and_listen(&pool, &tx, &stop, &stop_notify).await {
            Ok(()) => break, // Clean exit (stop flag set)
            Err(e) => {
                eprintln!("Listener error: {e}. Reconnecting in {RECONNECT_DELAY_SECS}s...");
                let _ = tx.try_send(Event::ListenerStateChanged(ListenerState::Reconnecting));
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(RECONNECT_DELAY_SECS)) => {}
                    _ = stop_notify.notified() => {}
                }
            }
        }
    }

    let _ = tx.try_send(Event::ListenerStateChanged(ListenerState::Disconnected));
}

/// Connect to PostgreSQL and listen for notifications with debouncing.
async fn connect_and_listen(
    pool: &PgPool,
    tx: &Sender<Event>,
    stop: &Arc<AtomicBool>,
    stop_notify: &Arc<Notify>,
) -> Result<(), sqlx::Error> {
    if stop.load(Ordering::Relaxed) {
        return Ok(());
    }
    let _ = tx.try_send(Event::ListenerStateChanged(ListenerState::Connecting));

    let mut listener = PgListener::connect_with(pool).await?;
    for ch in CHANNELS {
        listener.listen(ch).await?;
    }

    if stop.load(Ordering::Relaxed) {
        return Ok(());
    }
    let _ = tx.try_send(Event::ListenerStateChanged(ListenerState::Connected));

    let mut batch = NotifyBatch::default();
    let mut deadline: Option<Instant> = None;

    loop {
        if stop.load(Ordering::Relaxed) {
            return Ok(());
        }

        let timeout = deadline
            .map(|d| d.saturating_duration_since(Instant::now()))
            .unwrap_or(Duration::from_secs(60));

        tokio::select! {
            _ = stop_notify.notified() => {
                return Ok(());
            }
            result = listener.recv() => {
                let notif = result?;
                match notif.channel() {
                    "horsies_task_status" => batch.task_status = true,
                    "horsies_workflow_status" => batch.workflow_status = true,
                    "horsies_worker_state" => batch.worker_state = true,
                    _ => {}
                }
                if deadline.is_none() {
                    deadline = Some(Instant::now() + Duration::from_millis(DEBOUNCE_MS));
                }
            }
            _ = tokio::time::sleep(timeout) => {
                if batch.task_status || batch.workflow_status || batch.worker_state {
                    let _ = tx.try_send(Event::DbNotify(batch.clone()));
                    batch = NotifyBatch::default();
                }
                deadline = None;
            }
        }
    }
}
