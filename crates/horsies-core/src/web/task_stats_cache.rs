//! Short-lived shared cache for the monitoring aggregate reads: task stats,
//! facets and breakdown (Python `tests/dualrun/reference/web_read_cache.py:34-37`,
//! `:170-194`). Each read has its own cache, keyed by its query text.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;

use crate::monitoring::StatusCount;

/// How long a cached aggregate answers, for every aggregate read.
pub const TASK_STATS_CACHE_TTL: Duration = Duration::from_secs(10);
const TASK_STATS_CACHE_MAX_ENTRIES: usize = 256;
const TASK_STATS_REFRESH_SHARDS: usize = 16;

#[derive(Debug, Clone)]
struct CachedRead<T> {
    value: T,
    expires_at: Instant,
}

/// A cache of one aggregate read: at most 256 scopes, each answered for
/// [`TASK_STATS_CACHE_TTL`]; concurrent misses of one scope share one load,
/// and a failed load is not cached.
#[derive(Debug)]
pub(crate) struct ReadCache<T> {
    ttl: Duration,
    max_entries: usize,
    entries: RwLock<HashMap<String, CachedRead<T>>>,
    refreshes: Box<[Mutex<()>]>,
}

/// The cache of the task stats read.
pub(crate) type TaskStatsCache = ReadCache<Vec<StatusCount>>;

impl<T: Clone> ReadCache<T> {
    pub(crate) fn new() -> Self {
        Self::with_config(TASK_STATS_CACHE_TTL, TASK_STATS_CACHE_MAX_ENTRIES)
    }

    fn with_config(ttl: Duration, max_entries: usize) -> Self {
        assert!(
            max_entries > 0,
            "task stats cache must hold at least one entry"
        );
        Self {
            ttl,
            max_entries,
            entries: RwLock::new(HashMap::new()),
            refreshes: (0..TASK_STATS_REFRESH_SHARDS)
                .map(|_| Mutex::new(()))
                .collect(),
        }
    }

    async fn get(&self, key: &str, now: Instant) -> Option<T> {
        self.entries
            .read()
            .await
            .get(key)
            .filter(|cached| now < cached.expires_at)
            .map(|cached| cached.value.clone())
    }

    fn refresh_shard(&self, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize % self.refreshes.len()
    }

    async fn insert(&self, key: String, value: T) {
        let now = Instant::now();
        let mut entries = self.entries.write().await;
        entries.retain(|_, cached| now < cached.expires_at);
        if entries.len() >= self.max_entries && !entries.contains_key(&key) {
            let oldest = entries
                .iter()
                .min_by_key(|(_, cached)| cached.expires_at)
                .map(|(key, _)| key.clone());
            if let Some(oldest) = oldest {
                entries.remove(&oldest);
            }
        }
        entries.insert(
            key,
            CachedRead {
                value,
                expires_at: now + self.ttl,
            },
        );
    }

    pub(crate) async fn get_or_try_init<E, F, Fut>(&self, key: String, load: F) -> Result<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        if let Some(value) = self.get(&key, Instant::now()).await {
            return Ok(value);
        }

        let shard = self.refresh_shard(&key);
        let _refresh = self.refreshes[shard].lock().await;
        if let Some(value) = self.get(&key, Instant::now()).await {
            return Ok(value);
        }

        let value = load().await?;
        self.insert(key, value.clone()).await;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use tokio::sync::Notify;

    use super::*;

    fn counts(count: i64) -> Vec<StatusCount> {
        vec![StatusCount {
            status: "COMPLETED".to_owned(),
            count,
        }]
    }

    /// The facets and breakdown reads use the same cache with their own value
    /// type: a second read of a scope within the TTL does not load again.
    #[tokio::test(start_paused = true)]
    async fn a_cache_of_another_read_answers_a_scope_once_within_the_ttl() {
        let cache: ReadCache<String> = ReadCache::new();
        let loads = AtomicUsize::new(0);
        for _ in 0..3 {
            let value = cache
                .get_or_try_init("status=FAILED".to_owned(), || async {
                    loads.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, ()>("facets".to_owned())
                })
                .await
                .expect("load");
            assert_eq!(value, "facets");
        }
        assert_eq!(loads.load(Ordering::SeqCst), 1);

        tokio::time::advance(TASK_STATS_CACHE_TTL).await;
        cache
            .get_or_try_init("status=FAILED".to_owned(), || async {
                loads.fetch_add(1, Ordering::SeqCst);
                Ok::<_, ()>("facets".to_owned())
            })
            .await
            .expect("load");
        assert_eq!(loads.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn entries_expire_at_the_ttl_and_failed_loads_are_not_cached() {
        let cache = TaskStatsCache::with_config(Duration::from_secs(10), 2);
        let loads = AtomicUsize::new(0);

        let first = cache
            .get_or_try_init("all".to_owned(), || async {
                loads.fetch_add(1, Ordering::Relaxed);
                Ok::<_, ()>(counts(1))
            })
            .await
            .unwrap();
        let cached = cache
            .get_or_try_init("all".to_owned(), || async {
                loads.fetch_add(1, Ordering::Relaxed);
                Ok::<_, ()>(counts(2))
            })
            .await
            .unwrap();
        assert_eq!(first, counts(1));
        assert_eq!(cached, first);
        assert_eq!(loads.load(Ordering::Relaxed), 1);

        tokio::time::advance(Duration::from_secs(10)).await;
        let refreshed = cache
            .get_or_try_init("all".to_owned(), || async {
                loads.fetch_add(1, Ordering::Relaxed);
                Ok::<_, ()>(counts(2))
            })
            .await
            .unwrap();
        assert_eq!(refreshed, counts(2));
        assert_eq!(loads.load(Ordering::Relaxed), 2);

        let error = cache
            .get_or_try_init("filtered".to_owned(), || async {
                loads.fetch_add(1, Ordering::Relaxed);
                Err::<Vec<StatusCount>, _>("unavailable")
            })
            .await;
        assert_eq!(error, Err("unavailable"));
        let recovered = cache
            .get_or_try_init("filtered".to_owned(), || async {
                loads.fetch_add(1, Ordering::Relaxed);
                Ok::<_, &str>(counts(3))
            })
            .await
            .unwrap();
        assert_eq!(recovered, counts(3));
        assert_eq!(loads.load(Ordering::Relaxed), 4);
    }

    #[tokio::test]
    async fn concurrent_reads_of_one_scope_share_one_load() {
        let cache = Arc::new(TaskStatsCache::new());
        let loads = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());

        let first = {
            let cache = Arc::clone(&cache);
            let loads = Arc::clone(&loads);
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            tokio::spawn(async move {
                cache
                    .get_or_try_init("all".to_owned(), || async move {
                        loads.fetch_add(1, Ordering::Relaxed);
                        started.notify_one();
                        release.notified().await;
                        Ok::<_, ()>(counts(7))
                    })
                    .await
            })
        };
        started.notified().await;
        let second = {
            let cache = Arc::clone(&cache);
            let loads = Arc::clone(&loads);
            tokio::spawn(async move {
                cache
                    .get_or_try_init("all".to_owned(), || async move {
                        loads.fetch_add(1, Ordering::Relaxed);
                        Ok::<_, ()>(counts(8))
                    })
                    .await
            })
        };
        tokio::task::yield_now().await;
        assert_eq!(loads.load(Ordering::Relaxed), 1);
        release.notify_one();

        assert_eq!(first.await.unwrap().unwrap(), counts(7));
        assert_eq!(second.await.unwrap().unwrap(), counts(7));
        assert_eq!(loads.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn entry_count_is_bounded_and_expired_entries_are_removed() {
        let cache = TaskStatsCache::with_config(Duration::from_secs(10), 2);
        for (key, count) in [("one", 1), ("two", 2), ("three", 3)] {
            cache
                .get_or_try_init(key.to_owned(), || async move { Ok::<_, ()>(counts(count)) })
                .await
                .unwrap();
            tokio::time::advance(Duration::from_millis(1)).await;
        }
        assert_eq!(cache.entries.read().await.len(), 2);

        tokio::time::advance(Duration::from_secs(10)).await;
        cache
            .get_or_try_init("four".to_owned(), || async { Ok::<_, ()>(counts(4)) })
            .await
            .unwrap();
        assert_eq!(cache.entries.read().await.len(), 1);
    }
}
