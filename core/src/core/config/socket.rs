//! TCP keepalives and the TCP user timeout of every core connection
//! (Python `PostgresConfig.tcp_keepalives`, `tcp_user_timeout`,
//! `clients/python/horsies/core/models/broker.py`).
//!
//! Each setting has two forms. The config form ([`input`]) is what
//! `PostgresConfig` holds: each count is a plain number, and AppConfig
//! validation refuses a value outside its bound
//! ([`crate::core::PostgresConfig::socket_value_errors`]). The checked form
//! (the enums of this module) carries each bound in a type that only its
//! `TryFrom<u32>` constructs: it is the form of the manifests
//! ([`crate::manifest::DatabaseManifest`]), so a manifest value out of
//! bounds is refused when the manifest is deserialized. Each bounded type
//! implements [`ManifestValue`] next to its parser, so the bound has one
//! definition. The defaults are the statement layer's
//! [`DEFAULT_SOCKET_SETTINGS`](crate::db::DEFAULT_SOCKET_SETTINGS), which are
//! Python's defaults.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::no_member_after_the_tag;
use crate::db::{Keepalives, SocketSettings, TcpUserTimeout};
use crate::manifest_schema::{IntBound, ManifestValue, ValueType};

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
    const VALUE_TYPE: ValueType = ValueType::Int(IntBound {
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
    const VALUE_TYPE: ValueType = ValueType::Int(IntBound {
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
    const VALUE_TYPE: ValueType = ValueType::Int(IntBound {
        name: "TCP_USER_TIMEOUT_MS",
        min: 1,
        max: MAX_TCP_USER_TIMEOUT_MS as u64,
    });
}

/// TCP keepalives of every core connection, checked form (Python
/// `TcpKeepalivesOff`, `TcpKeepalivesOn`).
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, horsies_core_derive::ManifestSchema,
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

/// The TCP user timeout of every core connection, checked form (Python
/// `TcpUserTimeoutSystemDefault`, `TcpUserTimeoutAfter`).
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, horsies_core_derive::ManifestSchema,
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
    keepalives: input::TcpKeepalivesConfig,
    tcp_user_timeout: input::TcpUserTimeoutConfig,
) -> SocketSettings {
    SocketSettings {
        keepalives: keepalives.keepalives(),
        tcp_user_timeout: tcp_user_timeout.tcp_user_timeout(),
    }
}

/// The config forms of the two settings, as `PostgresConfig` holds them:
/// each count is a plain number. A number outside its bound parses;
/// AppConfig validation refuses it, and [`TcpKeepalivesConfig::checked`]
/// and [`TcpUserTimeoutConfig::checked`] build the checked form of the
/// manifests. The JSON form is the form of the checked types.
pub mod input {
    use std::time::Duration;

    use serde::{Deserialize, Serialize};

    use crate::core::config::no_member_after_the_tag;
    use crate::db::{Keepalives, TcpUserTimeout};

    use super::{KeepaliveProbes, KeepaliveSeconds, TcpUserTimeoutMs};

    /// A socket setting value outside its bound. `field` is the path of the
    /// value in `PostgresConfig`; `cause` names the bound, never the value.
    #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
    #[error("{field}: {cause}")]
    pub struct SocketValueRefused {
        pub field: &'static str,
        pub cause: String,
    }

    /// TCP keepalives of every core connection (Python `TcpKeepalivesOff`,
    /// `TcpKeepalivesOn`). Default: on, 30 s idle, 10 s between probes, 3
    /// probes.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
    pub enum TcpKeepalivesConfig {
        #[serde(deserialize_with = "no_member_after_the_tag")]
        Off,
        On {
            /// Idle time before the first probe (libpq `keepalives_idle`):
            /// 1 to [`super::MAX_KEEPALIVE_SECONDS`] seconds.
            idle_seconds: u32,
            /// Time between probes (libpq `keepalives_interval`): 1 to
            /// [`super::MAX_KEEPALIVE_SECONDS`] seconds.
            interval_seconds: u32,
            /// Unanswered probes before the drop (libpq
            /// `keepalives_count`): 1 to [`super::MAX_KEEPALIVE_PROBES`].
            probes: u32,
        },
    }

    impl Default for TcpKeepalivesConfig {
        fn default() -> Self {
            super::TcpKeepalivesConfig::default().into()
        }
    }

    impl From<super::TcpKeepalivesConfig> for TcpKeepalivesConfig {
        fn from(checked: super::TcpKeepalivesConfig) -> Self {
            match checked {
                super::TcpKeepalivesConfig::Off => Self::Off,
                super::TcpKeepalivesConfig::On {
                    idle_seconds,
                    interval_seconds,
                    probes,
                } => Self::On {
                    idle_seconds: idle_seconds.get(),
                    interval_seconds: interval_seconds.get(),
                    probes: probes.get(),
                },
            }
        }
    }

    impl TcpKeepalivesConfig {
        /// The checked form, or the first value outside its bound, in field
        /// order. Pure.
        pub fn checked(self) -> Result<super::TcpKeepalivesConfig, SocketValueRefused> {
            match self {
                Self::Off => Ok(super::TcpKeepalivesConfig::Off),
                Self::On {
                    idle_seconds,
                    interval_seconds,
                    probes,
                } => Ok(super::TcpKeepalivesConfig::On {
                    idle_seconds: bounded::<KeepaliveSeconds>(
                        "tcp_keepalives.idle_seconds",
                        idle_seconds,
                    )?,
                    interval_seconds: bounded::<KeepaliveSeconds>(
                        "tcp_keepalives.interval_seconds",
                        interval_seconds,
                    )?,
                    probes: bounded::<KeepaliveProbes>("tcp_keepalives.probes", probes)?,
                }),
            }
        }

        /// The keepalives of the statement layer. A config that AppConfig
        /// validation accepted holds values inside their bounds.
        pub fn keepalives(self) -> Keepalives {
            match self {
                Self::Off => Keepalives::Off,
                Self::On {
                    idle_seconds,
                    interval_seconds,
                    probes,
                } => Keepalives::On {
                    idle: Duration::from_secs(u64::from(idle_seconds)),
                    interval: Duration::from_secs(u64::from(interval_seconds)),
                    probes,
                },
            }
        }
    }

    /// The TCP user timeout of every core connection (Python
    /// `TcpUserTimeoutSystemDefault`, `TcpUserTimeoutAfter`). Default:
    /// 30,000 ms.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
    pub enum TcpUserTimeoutConfig {
        /// The operating system's timeout.
        #[serde(deserialize_with = "no_member_after_the_tag")]
        SystemDefault,
        /// Sent data may stay unacknowledged this long before the kernel
        /// closes the connection (libpq `tcp_user_timeout`): 1 to
        /// [`super::MAX_TCP_USER_TIMEOUT_MS`] ms.
        After { milliseconds: u32 },
    }

    impl Default for TcpUserTimeoutConfig {
        fn default() -> Self {
            super::TcpUserTimeoutConfig::default().into()
        }
    }

    impl From<super::TcpUserTimeoutConfig> for TcpUserTimeoutConfig {
        fn from(checked: super::TcpUserTimeoutConfig) -> Self {
            match checked {
                super::TcpUserTimeoutConfig::SystemDefault => Self::SystemDefault,
                super::TcpUserTimeoutConfig::After { milliseconds } => Self::After {
                    milliseconds: milliseconds.get(),
                },
            }
        }
    }

    impl TcpUserTimeoutConfig {
        /// The checked form, or the milliseconds outside their bound. Pure.
        pub fn checked(self) -> Result<super::TcpUserTimeoutConfig, SocketValueRefused> {
            match self {
                Self::SystemDefault => Ok(super::TcpUserTimeoutConfig::SystemDefault),
                Self::After { milliseconds } => Ok(super::TcpUserTimeoutConfig::After {
                    milliseconds: bounded::<TcpUserTimeoutMs>(
                        "tcp_user_timeout.milliseconds",
                        milliseconds,
                    )?,
                }),
            }
        }

        /// The TCP user timeout of the statement layer. A config that
        /// AppConfig validation accepted holds a value inside its bound.
        pub fn tcp_user_timeout(self) -> TcpUserTimeout {
            match self {
                Self::SystemDefault => TcpUserTimeout::SystemDefault,
                Self::After { milliseconds } => {
                    TcpUserTimeout::After(Duration::from_millis(u64::from(milliseconds)))
                }
            }
        }
    }

    /// `value` as the bounded type `T`, or the refusal of `field`. Pure.
    fn bounded<T: TryFrom<u32, Error = String>>(
        field: &'static str,
        value: u32,
    ) -> Result<T, SocketValueRefused> {
        T::try_from(value).map_err(|cause| SocketValueRefused { field, cause })
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
                input::TcpKeepalivesConfig::default(),
                input::TcpUserTimeoutConfig::default()
            ),
            DEFAULT_SOCKET_SETTINGS
        );
    }

    #[test]
    fn a_config_value_outside_its_bound_is_refused_with_its_field() {
        let refused = input::TcpKeepalivesConfig::On {
            idle_seconds: 30,
            interval_seconds: MAX_KEEPALIVE_SECONDS + 1,
            probes: 0,
        }
        .checked()
        .expect_err("the interval is above its bound");
        assert_eq!(refused.field, "tcp_keepalives.interval_seconds");
        let refused = input::TcpUserTimeoutConfig::After { milliseconds: 0 }
            .checked()
            .expect_err("zero milliseconds are below the bound");
        assert_eq!(refused.field, "tcp_user_timeout.milliseconds");
    }

    #[test]
    fn a_config_value_inside_its_bound_reaches_the_checked_form() {
        let config = input::TcpKeepalivesConfig::On {
            idle_seconds: 45,
            interval_seconds: 7,
            probes: MAX_KEEPALIVE_PROBES,
        };
        let checked = config.checked().expect("every value is in bounds");
        assert_eq!(input::TcpKeepalivesConfig::from(checked), config);
        assert_eq!(checked.keepalives(), config.keepalives());
        let timeout = input::TcpUserTimeoutConfig::After { milliseconds: 1500 };
        let checked = timeout.checked().expect("1500 ms is in bounds");
        assert_eq!(input::TcpUserTimeoutConfig::from(checked), timeout);
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
            input::TcpKeepalivesConfig::Off,
            input::TcpUserTimeoutConfig::SystemDefault,
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
