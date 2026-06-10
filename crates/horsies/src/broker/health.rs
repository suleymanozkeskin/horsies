//! Typed result models for the worker/database health API.
//!
//! Mirrors Python's `horsies/core/models/health.py`. These carry no behaviour,
//! only observed data returned to callers from the broker's health methods.

/// Result of a `SELECT 1` round-trip through the live broker pool.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DatabasePing {
    /// Measured round-trip latency in milliseconds.
    pub latency_ms: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_ping_holds_latency() {
        let ping = DatabasePing { latency_ms: 1.5 };
        assert_eq!(ping.latency_ms, 1.5);
    }
}
