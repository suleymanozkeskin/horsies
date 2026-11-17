//! TCP keepalives and the TCP user timeout of every core connection, as
//! config values (Python `PostgresConfig.tcp_keepalives`,
//! `tcp_keepalives_idle`, `tcp_keepalives_interval`, `tcp_keepalives_count`,
//! `tcp_user_timeout_ms`, `python/horsies/core/models/broker.py:129-157`).
//!
//! Each count is a bounded type that only its `TryFrom<u32>` constructs, so
//! a value out of bounds is refused when the config or the manifest is
//! deserialized. The defaults are the statement layer's
//! [`DEFAULT_SOCKET_SETTINGS`](crate::db::DEFAULT_SOCKET_SETTINGS), which are
//! Python's defaults. The two config enums are manifest types
//! ([`crate::manifest::DatabaseManifest`]); each bounded type implements
//! [`ManifestValue`] next to its parser, so the bound has one definition.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::no_member_after_the_tag;
use crate::db::{Keepalives, SocketSettings, TcpUserTimeout};
use crate::manifest_schema::{IntBound, ManifestValue, PythonValue};

/// Largest keepalive idle time and interval in seconds: Linux's limit for
/// `TCP_KEEPIDLE` and `TCP_KEEPINTVL` (tcp(7)).
pub const MAX_KEEPALIVE_SECONDS: u32 = 32_767;

/// Largest count of keepalive probes: Linux's limit for `TCP_KEEPCNT`
/// (tcp(7)).
pub const MAX_KEEPALIVE_PROBES: u32 = 127;

/// Largest TCP user timeout in milliseconds: `TCP_USER_TIMEOUT` takes a C
/// `int`.
pub const MAX_TCP_USER_TIMEOUT_MS: u32 = 2_147_483_647;

/// Seconds of a keepalive setting, from 1 to [`MAX_KEEPALIVE_SECONDS`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct KeepaliveSeconds(u32);

impl KeepaliveSeconds {
    pub const fn get(self) -> u32 {
        self.0
    }

    pub fn as_duration(self) -> Duration {
        Duration::from_secs(u64::from(self.0))
    }
}

impl TryFrom<u32> for KeepaliveSeconds {
    type Error = String;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            1..=MAX_KEEPALIVE_SECONDS => Ok(Self(value)),
            _ => Err(format!(
                "a keepalive time must be from 1 to {MAX_KEEPALIVE_SECONDS} seconds"
            )),
        }
    }
}

impl From<KeepaliveSeconds> for u32 {
    fn from(seconds: KeepaliveSeconds) -> Self {
        seconds.0
    }
}

impl ManifestValue for KeepaliveSeconds {
    const PYTHON: PythonValue = PythonValue::Int(IntBound {
        name: "KEEPALIVE_SECONDS",
        min: 1,
        max: MAX_KEEPALIVE_SECONDS as u64,
    });
}

/// Unanswered keepalive probes before the kernel drops the connection, from 1
/// to [`MAX_KEEPALIVE_PROBES`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct KeepaliveProbes(u32);

impl KeepaliveProbes {
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl TryFrom<u32> for KeepaliveProbes {
    type Error = String;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            1..=MAX_KEEPALIVE_PROBES => Ok(Self(value)),
            _ => Err(format!(
                "a keepalive probe count must be from 1 to {MAX_KEEPALIVE_PROBES}"
            )),
        }
    }
}

impl From<KeepaliveProbes> for u32 {
    fn from(probes: KeepaliveProbes) -> Self {
        probes.0
    }
}

impl ManifestValue for KeepaliveProbes {
    const PYTHON: PythonValue = PythonValue::Int(IntBound {
        name: "KEEPALIVE_PROBES",
        min: 1,
        max: MAX_KEEPALIVE_PROBES as u64,
    });
}

/// A TCP user timeout in milliseconds, from 1 to [`MAX_TCP_USER_TIMEOUT_MS`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct TcpUserTimeoutMs(u32);

impl TcpUserTimeoutMs {
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl TryFrom<u32> for TcpUserTimeoutMs {
    type Error = String;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            1..=MAX_TCP_USER_TIMEOUT_MS => Ok(Self(value)),
            _ => Err(format!(
                "a TCP user timeout must be from 1 to {MAX_TCP_USER_TIMEOUT_MS} ms"
            )),
        }
    }
}

impl From<TcpUserTimeoutMs> for u32 {
    fn from(milliseconds: TcpUserTimeoutMs) -> Self {
        milliseconds.0
    }
}

impl ManifestValue for TcpUserTimeoutMs {
    const PYTHON: PythonValue = PythonValue::Int(IntBound {
        name: "TCP_USER_TIMEOUT_MS",
        min: 1,
        max: MAX_TCP_USER_TIMEOUT_MS as u64,
    });
}

/// TCP keepalives of every core connection. Python's
/// `tcp_keepalives=False` is `Off`; `True` is `On` with the three values.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum TcpKeepalivesConfig {
    #[serde(deserialize_with = "no_member_after_the_tag")]
    Off,
    On {
        /// Idle time before the first probe (libpq `keepalives_idle`).
        idle_seconds: KeepaliveSeconds,
        /// Time between probes (libpq `keepalives_interval`).
        interval_seconds: KeepaliveSeconds,
        /// Unanswered probes before the drop (libpq `keepalives_count`).
        probes: KeepaliveProbes,
    },
}

/// Python's defaults: on, 30 s idle, 10 s interval, 3 probes.
impl Default for TcpKeepalivesConfig {
    fn default() -> Self {
        TcpKeepalivesConfig::On {
            idle_seconds: KeepaliveSeconds(30),
            interval_seconds: KeepaliveSeconds(10),
            probes: KeepaliveProbes(3),
        }
    }
}

impl TcpKeepalivesConfig {
    /// The keepalives of the statement layer.
    pub fn keepalives(self) -> Keepalives {
        match self {
            TcpKeepalivesConfig::Off => Keepalives::Off,
            TcpKeepalivesConfig::On {
                idle_seconds,
                interval_seconds,
                probes,
            } => Keepalives::On {
                idle: idle_seconds.as_duration(),
                interval: interval_seconds.as_duration(),
                probes: probes.get(),
            },
        }
    }
}

/// The TCP user timeout of every core connection. Python's
/// `tcp_user_timeout_ms=0` is `SystemDefault`; another value is `After`.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum TcpUserTimeoutConfig {
    /// The operating system's timeout.
    #[serde(deserialize_with = "no_member_after_the_tag")]
    SystemDefault,
    /// Sent data may stay unacknowledged this long before the kernel closes
    /// the connection (libpq `tcp_user_timeout`).
    After { milliseconds: TcpUserTimeoutMs },
}

/// Python's default: 30,000 ms.
impl Default for TcpUserTimeoutConfig {
    fn default() -> Self {
        TcpUserTimeoutConfig::After {
            milliseconds: TcpUserTimeoutMs(30_000),
        }
    }
}

impl TcpUserTimeoutConfig {
    /// The TCP user timeout of the statement layer.
    pub fn tcp_user_timeout(self) -> TcpUserTimeout {
        match self {
            TcpUserTimeoutConfig::SystemDefault => TcpUserTimeout::SystemDefault,
            TcpUserTimeoutConfig::After { milliseconds } => {
                TcpUserTimeout::After(Duration::from_millis(u64::from(milliseconds.get())))
            }
        }
    }
}

/// The socket settings of the statement layer from the two config values.
pub fn socket_settings(
    keepalives: TcpKeepalivesConfig,
    tcp_user_timeout: TcpUserTimeoutConfig,
) -> SocketSettings {
    SocketSettings {
        keepalives: keepalives.keepalives(),
        tcp_user_timeout: tcp_user_timeout.tcp_user_timeout(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DEFAULT_SOCKET_SETTINGS;

    #[test]
    fn the_defaults_are_the_statement_layer_s() {
        assert_eq!(
            socket_settings(
                TcpKeepalivesConfig::default(),
                TcpUserTimeoutConfig::default()
            ),
            DEFAULT_SOCKET_SETTINGS
        );
    }

    #[test]
    fn values_out_of_bounds_are_refused() {
        for refused in [0, MAX_KEEPALIVE_SECONDS + 1] {
            assert!(KeepaliveSeconds::try_from(refused).is_err(), "{refused}");
        }
        assert!(KeepaliveSeconds::try_from(MAX_KEEPALIVE_SECONDS).is_ok());
        for refused in [0, MAX_KEEPALIVE_PROBES + 1] {
            assert!(KeepaliveProbes::try_from(refused).is_err(), "{refused}");
        }
        assert!(KeepaliveProbes::try_from(MAX_KEEPALIVE_PROBES).is_ok());
        for refused in [0, MAX_TCP_USER_TIMEOUT_MS + 1] {
            assert!(TcpUserTimeoutMs::try_from(refused).is_err(), "{refused}");
        }
        assert!(TcpUserTimeoutMs::try_from(MAX_TCP_USER_TIMEOUT_MS).is_ok());
    }

    #[test]
    fn off_and_the_system_default_reach_the_statement_layer() {
        let settings = socket_settings(
            TcpKeepalivesConfig::Off,
            TcpUserTimeoutConfig::SystemDefault,
        );

        assert_eq!(settings.keepalives, Keepalives::Off);
        assert_eq!(settings.tcp_user_timeout, TcpUserTimeout::SystemDefault);
    }

    #[test]
    fn the_wire_forms_are_tagged_and_bounded() {
        let parsed: TcpKeepalivesConfig = serde_json::from_str(
            r#"{"kind": "ON", "idle_seconds": 60, "interval_seconds": 5, "probes": 4}"#,
        )
        .expect("a valid keepalive config");
        assert_eq!(
            parsed.keepalives(),
            Keepalives::On {
                idle: Duration::from_secs(60),
                interval: Duration::from_secs(5),
                probes: 4,
            }
        );
        let parsed: TcpUserTimeoutConfig =
            serde_json::from_str(r#"{"kind": "AFTER", "milliseconds": 1500}"#)
                .expect("a valid timeout config");
        assert_eq!(
            parsed.tcp_user_timeout(),
            TcpUserTimeout::After(Duration::from_millis(1500))
        );
        assert!(serde_json::from_str::<TcpKeepalivesConfig>(
            r#"{"kind": "ON", "idle_seconds": 0, "interval_seconds": 5, "probes": 4}"#
        )
        .is_err());
        assert!(serde_json::from_str::<TcpUserTimeoutConfig>(r#"{"kind": "OFF"}"#).is_err());
    }
}
