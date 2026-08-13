//! Process-monotonic UUIDv7 task identities.

use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use rand::RngCore;
use uuid::{Uuid, Version};

const MAX_UNIX_MILLISECONDS: i64 = (1_i64 << 48) - 1;
const MAX_COUNTER: u16 = (1_u16 << 12) - 1;
const RAND_B_MASK: u64 = (1_u64 << 62) - 1;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Uuid7Error {
    #[error("millisecond clock is outside the 48-bit range")]
    ClockOutOfRange,
    #[error("entropy source exceeded 62 bits")]
    EntropyOutOfRange,
}

pub struct MonotonicUuid7Generator {
    clock_ms: Box<dyn FnMut() -> i64 + Send>,
    entropy_62_bits: Box<dyn FnMut() -> u64 + Send>,
    last_ms: i64,
    counter: u16,
}

impl MonotonicUuid7Generator {
    pub fn new(
        clock_ms: impl FnMut() -> i64 + Send + 'static,
        entropy_62_bits: impl FnMut() -> u64 + Send + 'static,
    ) -> Self {
        Self {
            clock_ms: Box::new(clock_ms),
            entropy_62_bits: Box::new(entropy_62_bits),
            last_ms: -1,
            counter: 0,
        }
    }

    pub fn system() -> Self {
        Self::new(system_clock_ms, system_entropy_62_bits)
    }

    pub fn mint(&mut self) -> Result<Uuid, Uuid7Error> {
        let (milliseconds, counter) = self.advance()?;
        let entropy = (self.entropy_62_bits)();
        if entropy > RAND_B_MASK {
            return Err(Uuid7Error::EntropyOutOfRange);
        }
        let value = ((milliseconds as u128) << 80)
            | (0x7_u128 << 76)
            | ((counter as u128) << 64)
            | (0b10_u128 << 62)
            | entropy as u128;
        Ok(Uuid::from_u128(value))
    }

    fn advance(&mut self) -> Result<(i64, u16), Uuid7Error> {
        let mut now = (self.clock_ms)();
        validate_milliseconds(now)?;
        if now > self.last_ms {
            self.last_ms = now;
            self.counter = 0;
            return Ok((now, 0));
        }
        if self.counter < MAX_COUNTER {
            self.counter += 1;
            return Ok((self.last_ms, self.counter));
        }
        while now <= self.last_ms {
            now = (self.clock_ms)();
        }
        validate_milliseconds(now)?;
        self.last_ms = now;
        self.counter = 0;
        Ok((now, 0))
    }
}

fn validate_milliseconds(milliseconds: i64) -> Result<(), Uuid7Error> {
    if !(0..=MAX_UNIX_MILLISECONDS).contains(&milliseconds) {
        return Err(Uuid7Error::ClockOutOfRange);
    }
    Ok(())
}

fn system_clock_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before the Unix epoch")
        .as_millis() as i64
}

fn system_entropy_62_bits() -> u64 {
    rand::thread_rng().next_u64() >> 2
}

static PROCESS_GENERATOR: OnceLock<Mutex<MonotonicUuid7Generator>> = OnceLock::new();

pub fn mint_task_id() -> Result<Uuid, Uuid7Error> {
    PROCESS_GENERATOR
        .get_or_init(|| Mutex::new(MonotonicUuid7Generator::system()))
        .lock()
        .expect("UUIDv7 generator mutex poisoned")
        .mint()
}

pub fn uuid7_birth_at(value: Uuid) -> Option<DateTime<Utc>> {
    if value.get_version() != Some(Version::SortRand) {
        return None;
    }
    DateTime::from_timestamp_millis((value.as_u128() >> 80) as i64)
}

pub fn uuid7_birth_at_str(value: &str) -> Result<Option<DateTime<Utc>>, uuid::Error> {
    Uuid::parse_str(value).map(uuid7_birth_at)
}
