use crate::core::config::resilience::WorkerResilienceConfig;
use rand::Rng;

/// Stateful exponential backoff tracker with jitter.
///
/// Mirrors Python's `_RetryBackoff` class. Computes delays as
/// `min(max_ms, initial_ms * 2^(attempts-1))` with ±25% jitter,
/// floored at 0.1 seconds.
pub struct RetryBackoff {
    initial_ms: u64,
    max_ms: u64,
    max_attempts: u64,
    attempts: u64,
}

impl RetryBackoff {
    /// Create a backoff tracker from resilience config.
    pub fn from_config(config: &WorkerResilienceConfig) -> Self {
        Self {
            initial_ms: config.db_retry_initial_ms,
            max_ms: config.db_retry_max_ms,
            max_attempts: config.db_retry_max_attempts,
            attempts: 0,
        }
    }

    /// Reset the attempt counter (call after a successful operation).
    pub fn reset(&mut self) {
        self.attempts = 0;
    }

    /// Whether another retry is allowed.
    ///
    /// Always returns `true` when `max_attempts` is 0 (infinite retries).
    pub fn can_retry(&self) -> bool {
        self.max_attempts == 0 || self.attempts < self.max_attempts
    }

    /// Compute the next delay in seconds, incrementing the attempt counter.
    ///
    /// Formula: `min(max_ms, initial_ms * 2^(attempts-1))` with ±25% jitter,
    /// floored at 0.1 seconds.
    pub fn next_delay_seconds(&mut self) -> f64 {
        self.attempts += 1;

        let exponent = (self.attempts - 1).min(63) as u32;
        let multiplier = 1u64.checked_shl(exponent).unwrap_or(u64::MAX);
        let base_ms = self.initial_ms.saturating_mul(multiplier);
        let capped_ms = base_ms.min(self.max_ms);

        // Apply ±25% jitter.
        let jitter_factor = {
            let mut rng = rand::thread_rng();
            rng.gen_range(0.75..=1.25)
        };
        let jittered_ms = (capped_ms as f64) * jitter_factor;

        // Floor at 100ms (0.1s).
        let floored_ms = jittered_ms.max(100.0);

        floored_ms / 1000.0
    }

    /// Current number of attempts.
    pub fn attempts(&self) -> u64 {
        self.attempts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> WorkerResilienceConfig {
        WorkerResilienceConfig::default()
    }

    #[test]
    fn from_config_uses_defaults() {
        let backoff = RetryBackoff::from_config(&default_config());
        assert_eq!(backoff.initial_ms, 500);
        assert_eq!(backoff.max_ms, 30_000);
        assert_eq!(backoff.max_attempts, 0);
        assert_eq!(backoff.attempts, 0);
    }

    #[test]
    fn can_retry_infinite_when_max_zero() {
        let backoff = RetryBackoff::from_config(&default_config());
        assert!(backoff.can_retry());
    }

    #[test]
    fn can_retry_limited() {
        let config = WorkerResilienceConfig {
            db_retry_max_attempts: 3,
            ..default_config()
        };
        let mut backoff = RetryBackoff::from_config(&config);
        assert!(backoff.can_retry());
        backoff.next_delay_seconds();
        assert!(backoff.can_retry());
        backoff.next_delay_seconds();
        assert!(backoff.can_retry());
        backoff.next_delay_seconds();
        assert!(!backoff.can_retry());
    }

    #[test]
    fn reset_clears_attempts() {
        let config = WorkerResilienceConfig {
            db_retry_max_attempts: 2,
            ..default_config()
        };
        let mut backoff = RetryBackoff::from_config(&config);
        backoff.next_delay_seconds();
        backoff.next_delay_seconds();
        assert!(!backoff.can_retry());
        backoff.reset();
        assert_eq!(backoff.attempts(), 0);
        assert!(backoff.can_retry());
    }

    #[test]
    fn delays_increase_exponentially() {
        let config = WorkerResilienceConfig {
            db_retry_initial_ms: 1_000,
            db_retry_max_ms: 300_000,
            ..default_config()
        };
        let mut backoff = RetryBackoff::from_config(&config);

        // Attempt 1: base = 1000ms * 2^0 = 1000ms
        let d1 = backoff.next_delay_seconds();
        // Attempt 2: base = 1000ms * 2^1 = 2000ms
        let d2 = backoff.next_delay_seconds();
        // Attempt 3: base = 1000ms * 2^2 = 4000ms
        let d3 = backoff.next_delay_seconds();

        // With ±25% jitter, d1 in [0.75, 1.25], d2 in [1.5, 2.5], d3 in [3.0, 5.0]
        assert!((0.75..=1.25).contains(&d1), "d1={}", d1);
        assert!((1.5..=2.5).contains(&d2), "d2={}", d2);
        assert!((3.0..=5.0).contains(&d3), "d3={}", d3);
    }

    #[test]
    fn delays_capped_at_max() {
        let config = WorkerResilienceConfig {
            db_retry_initial_ms: 10_000,
            db_retry_max_ms: 15_000,
            ..default_config()
        };
        let mut backoff = RetryBackoff::from_config(&config);

        // Attempt 1: base = 10000ms, capped at 15000ms -> 10000ms
        let _d1 = backoff.next_delay_seconds();
        // Attempt 2: base = 20000ms, capped at 15000ms -> 15000ms
        let d2 = backoff.next_delay_seconds();
        // Attempt 3: base = 40000ms, capped at 15000ms -> 15000ms
        let d3 = backoff.next_delay_seconds();

        // d2 and d3 should both be capped: [15000 * 0.75, 15000 * 1.25] = [11.25, 18.75]
        assert!((11.25..=18.75).contains(&d2), "d2={}", d2);
        assert!((11.25..=18.75).contains(&d3), "d3={}", d3);
    }

    #[test]
    fn floor_at_100ms() {
        let config = WorkerResilienceConfig {
            db_retry_initial_ms: 100,
            db_retry_max_ms: 500,
            ..default_config()
        };
        let mut backoff = RetryBackoff::from_config(&config);

        let d = backoff.next_delay_seconds();
        // base = 100ms, jitter [75ms, 125ms], floor 100ms -> [0.1, 0.125]
        assert!(d >= 0.1, "delay should be >= 0.1s, got {}", d);
    }

    #[test]
    fn attempts_counter_increments() {
        let mut backoff = RetryBackoff::from_config(&default_config());
        assert_eq!(backoff.attempts(), 0);
        backoff.next_delay_seconds();
        assert_eq!(backoff.attempts(), 1);
        backoff.next_delay_seconds();
        assert_eq!(backoff.attempts(), 2);
    }

    #[test]
    fn large_exponent_does_not_overflow() {
        let config = WorkerResilienceConfig {
            db_retry_initial_ms: 500,
            db_retry_max_ms: 30_000,
            db_retry_max_attempts: 0,
            ..default_config()
        };
        let mut backoff = RetryBackoff::from_config(&config);

        // Simulate many retries — should not panic from overflow.
        for _ in 0..100 {
            let d = backoff.next_delay_seconds();
            assert!(d >= 0.1, "delay must be >= 0.1s");
            assert!(d <= 37.5, "delay must be <= max_ms * 1.25 = 37.5s");
        }
    }
}
