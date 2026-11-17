//! Exact-byte scoped idempotency keys.

use chrono::Duration;
use sha2::{Digest, Sha256};

pub const IDEMPOTENCY_KEY_MAX_BYTES: usize = 255;
pub const IDEMPOTENCY_SCOPE_VERSION: i16 = 1;
pub const IDEMPOTENCY_WINDOW_DEFAULT_HOURS: i64 = 24;
/// [`IDEMPOTENCY_WINDOW_DEFAULT_HOURS`] in microseconds, the unit of the
/// window that the database stores (`interval`).
pub const IDEMPOTENCY_WINDOW_DEFAULT_MICROS: i64 =
    IDEMPOTENCY_WINDOW_DEFAULT_HOURS * 3_600 * 1_000_000;
pub const IDEMPOTENCY_WINDOW_MAX_DAYS: i64 = 30;
const SCOPE_DOMAIN_V1: &[u8] = b"horsies.enqueue-key.v1";

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdempotencyKeyError {
    #[error("{label} must be non-empty")]
    Empty { label: &'static str },
    #[error("{label} must be at most 255 UTF-8 bytes")]
    OverBound { label: &'static str },
    #[error("idempotency reservation window must be positive")]
    NonPositiveWindow,
    #[error("idempotency reservation window exceeds the inclusive maximum of 30 days")]
    WindowOverBound,
    /// The database stores the window as an `interval`, exact to the
    /// microsecond; a finer window would be cut.
    #[error("idempotency reservation window must be a whole number of microseconds")]
    WindowFinerThanMicrosecond,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedIdempotencyKey {
    task_name: String,
    key: String,
}

impl ScopedIdempotencyKey {
    pub fn new(
        task_name: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<Self, IdempotencyKeyError> {
        let task_name = task_name.into();
        let key = key.into();
        validate_opaque("task_name", &task_name)?;
        validate_opaque("idempotency key", &key)?;
        Ok(Self { task_name, key })
    }

    pub fn digest(&self) -> [u8; 32] {
        digest_framed(
            SCOPE_DOMAIN_V1,
            &[self.task_name.as_bytes(), self.key.as_bytes()],
        )
    }

    pub fn task_name(&self) -> &str {
        &self.task_name
    }

    pub fn key(&self) -> &str {
        &self.key
    }
}

/// Checks a reservation window: more than zero, at most 30 days, and a
/// whole number of microseconds (the unit of the stored `interval`).
pub fn validate_reservation_window(window: Duration) -> Result<Duration, IdempotencyKeyError> {
    if window <= Duration::zero() {
        return Err(IdempotencyKeyError::NonPositiveWindow);
    }
    if window > Duration::days(IDEMPOTENCY_WINDOW_MAX_DAYS) {
        return Err(IdempotencyKeyError::WindowOverBound);
    }
    if window.subsec_nanos() % 1_000 != 0 {
        return Err(IdempotencyKeyError::WindowFinerThanMicrosecond);
    }
    Ok(window)
}

/// The microseconds of a reservation window, for the `interval` that the
/// database stores.
///
/// Errors: the refusals of [`validate_reservation_window`].
pub fn reservation_window_micros(window: Duration) -> Result<i64, IdempotencyKeyError> {
    let window = validate_reservation_window(window)?;
    Ok(window
        .num_microseconds()
        .expect("a window of at most 30 days fits an i64 count of microseconds"))
}

fn validate_opaque(label: &'static str, value: &str) -> Result<(), IdempotencyKeyError> {
    if value.is_empty() {
        return Err(IdempotencyKeyError::Empty { label });
    }
    if value.len() > IDEMPOTENCY_KEY_MAX_BYTES {
        return Err(IdempotencyKeyError::OverBound { label });
    }
    Ok(())
}

fn digest_framed(domain: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut digest = Sha256::new();
    update_framed(&mut digest, domain);
    for part in parts {
        update_framed(&mut digest, part);
    }
    digest.finalize().into()
}

fn update_framed(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u32).to_be_bytes());
    digest.update(value);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_window_keeps_its_microseconds() {
        assert_eq!(reservation_window_micros(Duration::microseconds(1)), Ok(1));
        assert_eq!(
            reservation_window_micros(Duration::milliseconds(500)),
            Ok(500_000)
        );
        assert_eq!(
            reservation_window_micros(Duration::milliseconds(1_500)),
            Ok(1_500_000)
        );
        assert_eq!(
            reservation_window_micros(Duration::days(IDEMPOTENCY_WINDOW_MAX_DAYS)),
            Ok(2_592_000_000_000)
        );
        assert_eq!(
            reservation_window_micros(Duration::hours(IDEMPOTENCY_WINDOW_DEFAULT_HOURS)),
            Ok(IDEMPOTENCY_WINDOW_DEFAULT_MICROS)
        );
    }

    #[test]
    fn a_window_outside_the_rule_is_refused() {
        assert_eq!(
            reservation_window_micros(Duration::zero()),
            Err(IdempotencyKeyError::NonPositiveWindow)
        );
        assert_eq!(
            reservation_window_micros(
                Duration::days(IDEMPOTENCY_WINDOW_MAX_DAYS) + Duration::microseconds(1)
            ),
            Err(IdempotencyKeyError::WindowOverBound)
        );
        assert_eq!(
            reservation_window_micros(Duration::nanoseconds(1_500)),
            Err(IdempotencyKeyError::WindowFinerThanMicrosecond)
        );
    }
}
