//! Deterministic simulation primitives.

use chrono::{DateTime, Timelike, Utc};
use chrono_tz::Tz;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkEnvelope {
    pub low_ms: u64,
    pub high_ms: u64,
}

impl WorkEnvelope {
    pub const fn new(low_ms: u64, high_ms: u64) -> Self {
        assert!(high_ms >= low_ms, "high_ms must be >= low_ms");
        Self { low_ms, high_ms }
    }
}

pub fn unit(parts: &[&str]) -> f64 {
    assert!(!parts.is_empty(), "unit() needs at least one part");
    let joined = parts.join("|");
    let digest = Sha256::digest(joined.as_bytes());
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    u64::from_be_bytes(bytes) as f64 / (u64::MAX as f64 + 1.0)
}

pub fn draw(rate: f64, parts: &[&str]) -> bool {
    assert!((0.0..=1.0).contains(&rate), "rate must be within [0,1]");
    unit(parts) < rate
}

pub fn integer(low: i64, high: i64, parts: &[&str]) -> i64 {
    assert!(high >= low, "high must be >= low");
    low + (unit(parts) * (high - low + 1) as f64) as i64
}

pub fn duration_ms(envelope: WorkEnvelope, parts: &[&str]) -> u64 {
    integer(envelope.low_ms as i64, envelope.high_ms as i64, parts) as u64
}

pub fn perform(envelope: WorkEnvelope, parts: &[&str]) -> u64 {
    let slept_ms = duration_ms(envelope, parts);
    std::thread::sleep(std::time::Duration::from_millis(slept_ms));
    slept_ms
}

pub fn stall(stall_ms: u64) -> u64 {
    std::thread::sleep(std::time::Duration::from_millis(stall_ms));
    stall_ms
}

pub fn demand_factor(epoch_seconds: f64) -> f64 {
    let seconds = epoch_seconds.max(0.0);
    let whole = seconds.floor() as i64;
    let nanos = ((seconds - whole as f64) * 1_000_000_000.0) as u32;
    let now = DateTime::<Utc>::from_timestamp(whole, nanos).expect("valid epoch");
    let zone: Tz = crate::tuning::STEADY_TIMEZONE
        .parse()
        .expect("valid showcase timezone");
    let local = now.with_timezone(&zone);
    let hour = local.hour() as f64 + f64::from(local.minute()) / 60.0;
    let low = crate::tuning::STEADY_HOURLY_DEMAND[local.hour() as usize];
    let high = crate::tuning::STEADY_HOURLY_DEMAND[(local.hour() as usize + 1) % 24];
    let base = low + (high - low) * (hour.fract());
    let ripple = 1.0
        + crate::tuning::STEADY_RIPPLE_AMPLITUDE
            * (std::f64::consts::TAU * seconds
                / (crate::tuning::STEADY_RIPPLE_PERIOD_MINUTES as f64 * 60.0))
                .sin();
    (base * ripple).max(0.05)
}

pub fn choice<T: Clone>(options: &[T], parts: &[&str]) -> T {
    assert!(!options.is_empty(), "choice() needs at least one option");
    options[(unit(parts) * options.len() as f64) as usize].clone()
}

pub fn sample<T: Clone>(options: &[T], count: usize, parts: &[&str]) -> Vec<T> {
    assert!(
        count <= options.len(),
        "cannot pick more entries than options"
    );
    let mut remaining = options.to_vec();
    let mut selected = Vec::with_capacity(count);
    for index in 0..count {
        let index_string = index.to_string();
        let mut keyed = parts.to_vec();
        keyed.push(&index_string);
        let position = (unit(&keyed) * remaining.len() as f64) as usize;
        selected.push(remaining.remove(position));
    }
    selected
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_parts_are_stable() {
        assert_eq!(unit(&["order-1", "psp"]), unit(&["order-1", "psp"]));
        assert_ne!(unit(&["order-1", "psp"]), unit(&["order-2", "psp"]));
        assert_eq!(integer(1, 4, &["same"]), integer(1, 4, &["same"]));
    }

    #[test]
    fn draws_have_the_configured_rate_within_sampling_error() {
        let rate = 0.20;
        let hits = (0..10_000)
            .filter(|index| draw(rate, &[&format!("order-{index}")]))
            .count();
        let observed = hits as f64 / 10_000.0;
        assert!(
            (observed - rate).abs() < 0.02,
            "observed {observed} for rate {rate}"
        );
    }

    #[test]
    fn configured_fault_rates_hold_over_seeded_domain_ids() {
        let configured = [
            (crate::tuning::PSP_UNAVAILABLE_RATE, "psp"),
            (crate::tuning::CARD_DECLINE_RATE, "card"),
            (crate::tuning::STOCK_SHORTFALL_RATE, "stock"),
            (crate::tuning::INVOICE_HANG_RATE, "invoice"),
            (crate::tuning::COURIER_FLAKE_RATE, "courier"),
            (crate::tuning::PROMOTION_BUNDLE_BUG_RATE, "bundle"),
            (crate::tuning::PROMOTION_SIZE_CODE_BUG_RATE, "size"),
            (crate::tuning::LOYALTY_ENGINE_BUG_RATE, "loyalty"),
            (crate::tuning::SUPPLIER_TIMEOUT_RATE, "supplier"),
            (crate::tuning::RETURN_DAMAGE_RATE, "return"),
            (crate::tuning::CDN_REJECT_RATE, "cdn"),
            (crate::tuning::ORIGIN_REJECT_RATE, "origin"),
            (crate::tuning::SEARCH_PREWARM_FAIL_RATE, "search"),
            (crate::tuning::CHAOS_EXPORT_CRASH_RATE, "export"),
        ];
        for (rate, label) in configured {
            let hits = (0..100_000)
                .filter(|index| draw(rate, &[&format!("seed-{index}"), label]))
                .count();
            let measured = hits as f64 / 100_000.0;
            assert!(
                (measured - rate).abs() < 0.01,
                "{label}: measured {measured} for configured rate {rate}"
            );
        }
    }

    #[test]
    fn envelopes_and_samples_are_bounded_and_repeatable() {
        let work = WorkEnvelope::new(2_000, 4_000);
        let first = duration_ms(work, &["order-1", "work"]);
        assert!((2_000..=4_000).contains(&first));
        assert_eq!(first, duration_ms(work, &["order-1", "work"]));
        let values = sample(&["a", "b", "c", "d"], 3, &["sample"]);
        assert_eq!(values, sample(&["a", "b", "c", "d"], 3, &["sample"]));
        assert_eq!(values.len(), 3);
    }
}
