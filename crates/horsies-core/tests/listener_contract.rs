//! The listener contract against a live PostgreSQL: ports of the core rows of
//! Python `tests/e2e/test_fanout_saturation.py`.
//!
//! Python's listener hands one queue per `listen()` call; the core's
//! [`Listener`] hands one [`PayloadSubscription`] per
//! subscription. The rows that pin Python's queue API (double unsubscribe,
//! unknown queue, unlisten) and the measurement rows are not core.
//!
//! `DATABASE_URL` names the server; each test creates its own empty database
//! through the core's test support (`src/test_support/database.rs`) and drops
//! it at the end, so NOTIFY traffic of other tests cannot reach it.

use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use horsies_core::db::{
    parse_connection_url, ChannelName, CheckoutClass, ConnectPlan, ConnectionAge,
    ConnectionEnvironment, CorePool, DatabasePassword, EnvironmentPassword, HomeDirectory,
    IdlePing, Listener, ListenerClose, ParameterType, PayloadEvent, PayloadSubscription, PoolPlan,
    PoolRole, PoolSizing, QueueCapacity, Reserve, StatementBound, StatementProtocol,
    TypedStatement, DEFAULT_SOCKET_SETTINGS,
};

const NOTIFY: TypedStatement = TypedStatement::new(
    "listener_contract_notify",
    "SELECT pg_notify($1, $2)",
    &[ParameterType::Text, ParameterType::Text],
);

const TASK_DONE: &str = "task_done";
const TASK_NEW: &str = "task_new";

/// The bound of each wait for an event that must come.
const EVENT_BOUND: Duration = Duration::from_secs(15);

/// How long a test waits to show that an event does not come.
const SILENCE_WINDOW: Duration = Duration::from_millis(500);

/// The queue size of an ordinary subscription in these tests.
const QUEUE_SIZE: usize = 64;

/// The most events a test reads while it looks for one payload.
const EVENT_READ_LIMIT: usize = 256;

fn test_environment() -> ConnectionEnvironment {
    let password = match std::env::var("PGPASSWORD") {
        Ok(text) if !text.is_empty() => {
            EnvironmentPassword::Set(DatabasePassword::from_bytes(text.into_bytes()))
        }
        Ok(_) | Err(_) => EnvironmentPassword::NotSet,
    };
    let home = match std::env::var_os("HOME") {
        Some(path) => HomeDirectory::Known(path.into()),
        None => HomeDirectory::Unknown,
    };
    ConnectionEnvironment::new(password, Vec::new(), home)
}

/// The test databases of the core (`src/test_support/database.rs`). This
/// binary creates empty databases only.
#[allow(dead_code)]
#[path = "../src/test_support/database.rs"]
mod test_database;

/// Installs the schema into a test template through the public API. This
/// binary creates empty databases only, so it never runs.
#[allow(dead_code)]
async fn install_template(url: &str) {
    let broker = horsies_core::PostgresBroker::connect(url, horsies_core::ClientFamily::Rust)
        .await
        .expect("connect the template broker");
    broker
        .migrate()
        .await
        .expect("install the schema into the test template");
    broker.close().await;
}

fn load_plan(url: &str) -> Arc<ConnectPlan> {
    let environment = test_environment();
    let settings = parse_connection_url(url, &environment).expect("test URL parses");
    Arc::new(
        ConnectPlan::load(&settings, &environment, &DEFAULT_SOCKET_SETTINGS)
            .expect("test plan loads"),
    )
}

/// A database of its own for one test, with a listener and a one-connection
/// pool that sends the notifications.
struct ListenerDatabase {
    database: test_database::TestDatabase,
    listener: Listener,
    pool: CorePool,
}

impl ListenerDatabase {
    async fn create() -> ListenerDatabase {
        let database = test_database::TestDatabase::create_empty().await;
        let url = database.url().to_owned();
        let listener = Listener::start(load_plan(&url));
        let pool = CorePool::new(
            PoolPlan {
                role: PoolRole::Main,
                sizing: PoolSizing::new(1, Reserve::NoReserve).expect("valid sizing"),
                checkout_wait: Duration::from_millis(500),
                idle_ping: IdlePing::AfterIdle(Duration::ZERO),
                max_age: ConnectionAge::Unlimited,
                protocol: StatementProtocol::UnnamedOnly,
            },
            load_plan(&url),
        );
        ListenerDatabase {
            database,
            listener,
            pool,
        }
    }

    async fn subscribe(&self, channel: &str, capacity: usize) -> PayloadSubscription {
        self.listener
            .subscribe_payloads(
                ChannelName::parse(channel).expect("valid channel"),
                QueueCapacity::new(capacity).expect("valid capacity"),
            )
            .await
            .expect("subscribe")
    }

    async fn notify(&self, channel: &str, payload: &str) {
        let channel = channel.to_owned();
        let payload = payload.to_owned();
        let _rows = self
            .pool
            .query(
                CheckoutClass::Reserved,
                &NOTIFY.bind().value(&channel).value(&payload).finish(),
                StatementBound::ConnectionTimeouts,
            )
            .await
            .expect("notify");
    }

    async fn remove(self) {
        assert_eq!(self.listener.close().await, ListenerClose::Closed);
        self.pool.close();
        self.database.drop().await;
    }
}

/// The next event of a subscription, within [`EVENT_BOUND`].
async fn next_event(subscription: &PayloadSubscription) -> PayloadEvent {
    tokio::time::timeout(EVENT_BOUND, subscription.recv())
        .await
        .expect("an event within the bound")
}

/// Whether the subscription reads no event within [`SILENCE_WINDOW`].
async fn stays_silent(subscription: &PayloadSubscription) -> bool {
    tokio::time::timeout(SILENCE_WINDOW, subscription.recv())
        .await
        .is_err()
}

/// Read events until `payload` arrives. Earlier payloads and gaps are
/// skipped; a stopped subscription or the read limit fails the test.
async fn read_until_payload(subscription: &PayloadSubscription, payload: &str) {
    for _ in 0..EVENT_READ_LIMIT {
        match next_event(subscription).await {
            PayloadEvent::Payload(text) if text == payload => return,
            PayloadEvent::Payload(_) | PayloadEvent::Overflowed | PayloadEvent::Gap => {}
            PayloadEvent::Stopped => panic!("the subscription stopped before {payload}"),
        }
    }
    panic!("{payload} did not arrive within {EVENT_READ_LIMIT} events");
}

/// Python `TestFanoutBoundary.test_zero_subscribers_notify_does_not_crash`:
/// a notification sent while the channel has no subscriber does not break
/// later delivery on that channel.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_notify_with_no_subscriber_does_not_break_later_delivery() {
    let database = ListenerDatabase::create().await;
    let first = database.subscribe(TASK_DONE, QUEUE_SIZE).await;
    drop(first);
    let unheard = Uuid::new_v4().to_string();
    database.notify(TASK_DONE, &unheard).await;

    let probe = database.subscribe(TASK_DONE, QUEUE_SIZE).await;
    let heard = Uuid::new_v4().to_string();
    database.notify(TASK_DONE, &heard).await;

    read_until_payload(&probe, &heard).await;
    drop(probe);
    database.remove().await;
}

/// Python `TestFanoutBoundary.test_single_subscriber_receives_notification`,
/// on the core listener: one subscriber, one notification, exactly one
/// payload.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_subscriber_gets_exactly_one_task_done_payload() {
    let database = ListenerDatabase::create().await;
    let subscription = database.subscribe(TASK_DONE, QUEUE_SIZE).await;
    let task_id = Uuid::new_v4().to_string();

    database.notify(TASK_DONE, &task_id).await;

    assert_eq!(
        next_event(&subscription).await,
        PayloadEvent::Payload(task_id)
    );
    assert!(
        stays_silent(&subscription).await,
        "one notification, one payload"
    );
    drop(subscription);
    database.remove().await;
}

/// Python `TestFanoutBoundary.test_unsubscribe_during_active_notifications`,
/// on the core listener: half of ten subscriptions are dropped while
/// notifications are in flight; the dispatch keeps working, and every
/// remaining subscription gets the notification sent after the drop. A
/// dropped subscription cannot be read, so the core cannot deliver to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriptions_dropped_during_a_burst_leave_the_others_receiving() {
    let database = ListenerDatabase::create().await;
    let mut subscriptions = Vec::new();
    for _ in 0..10 {
        subscriptions.push(database.subscribe(TASK_DONE, QUEUE_SIZE).await);
    }
    let remaining = subscriptions.split_off(5);
    let removed = subscriptions;
    for index in 0..10 {
        database.notify(TASK_DONE, &format!("burst-{index}")).await;
    }
    read_until_payload(&remaining[0], "burst-0").await;

    drop(removed);
    for index in 10..20 {
        database.notify(TASK_DONE, &format!("burst-{index}")).await;
    }
    let after_drop = Uuid::new_v4().to_string();
    database.notify(TASK_DONE, &after_drop).await;

    for subscription in &remaining {
        read_until_payload(subscription, &after_drop).await;
    }
    drop(remaining);
    database.remove().await;
}

/// Python `TestFanoutErrorPaths.test_bounded_queue_drops_on_full`: a full
/// subscription drops its oldest payload and reads `Overflowed` once; a
/// subscription with room gets every payload.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_subscription_drops_payloads_and_the_others_get_theirs() {
    let database = ListenerDatabase::create().await;
    let bounded = database.subscribe(TASK_DONE, 1).await;
    let control = database.subscribe(TASK_DONE, QUEUE_SIZE).await;
    let task_ids: Vec<String> = (0..3).map(|_| Uuid::new_v4().to_string()).collect();

    for task_id in &task_ids {
        database.notify(TASK_DONE, task_id).await;
    }

    for task_id in &task_ids {
        assert_eq!(
            next_event(&control).await,
            PayloadEvent::Payload(task_id.clone())
        );
    }
    assert_eq!(next_event(&bounded).await, PayloadEvent::Overflowed);
    assert_eq!(
        next_event(&bounded).await,
        PayloadEvent::Payload(task_ids[2].clone())
    );
    assert!(
        stays_silent(&bounded).await,
        "a queue of one holds one payload"
    );
    drop((bounded, control));
    database.remove().await;
}

/// Python `TestFanoutContracts.test_channel_isolation_no_cross_delivery`: a
/// notification on `task_done` does not reach a `task_new` subscription.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_notify_on_one_channel_does_not_reach_another_channel() {
    let database = ListenerDatabase::create().await;
    let done = database.subscribe(TASK_DONE, QUEUE_SIZE).await;
    let new = database.subscribe(TASK_NEW, QUEUE_SIZE).await;
    let task_id = Uuid::new_v4().to_string();

    database.notify(TASK_DONE, &task_id).await;

    assert_eq!(next_event(&done).await, PayloadEvent::Payload(task_id));
    assert!(
        stays_silent(&new).await,
        "a task_done notification reached a task_new subscription"
    );
    drop((done, new));
    database.remove().await;
}

/// Python `TestFanoutContracts.test_consuming_one_queue_does_not_affect_others`:
/// reading one subscription does not take or change the payloads of another.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reading_one_subscription_does_not_take_from_another() {
    let database = ListenerDatabase::create().await;
    let mut subscriptions = Vec::new();
    for _ in 0..3 {
        subscriptions.push(database.subscribe(TASK_DONE, QUEUE_SIZE).await);
    }
    let task_id = Uuid::new_v4().to_string();

    database.notify(TASK_DONE, &task_id).await;

    assert_eq!(
        next_event(&subscriptions[0]).await,
        PayloadEvent::Payload(task_id.clone())
    );
    assert!(stays_silent(&subscriptions[0]).await);
    for subscription in &subscriptions[1..] {
        assert_eq!(
            next_event(subscription).await,
            PayloadEvent::Payload(task_id.clone())
        );
    }
    drop(subscriptions);
    database.remove().await;
}
