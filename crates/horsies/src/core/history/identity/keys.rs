//! Exact-byte scoped idempotency keys.

use chrono::Duration;
use sha2::{Digest, Sha256};

pub const IDEMPOTENCY_KEY_MAX_BYTES: usize = 255;
pub const IDEMPOTENCY_SCOPE_VERSION: i16 = 1;
pub const IDEMPOTENCY_WINDOW_DEFAULT_HOURS: i64 = 24;
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

pub fn validate_reservation_window(window: Duration) -> Result<Duration, IdempotencyKeyError> {
    if window <= Duration::zero() {
        return Err(IdempotencyKeyError::NonPositiveWindow);
    }
    if window > Duration::days(IDEMPOTENCY_WINDOW_MAX_DAYS) {
        return Err(IdempotencyKeyError::WindowOverBound);
    }
    Ok(window)
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
