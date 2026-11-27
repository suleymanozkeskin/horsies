//! The address that a metrics endpoint binds, as a `--metrics-listen HOST:PORT`
//! argument gives it. A metrics endpoint has no
//! default address: the argument turns it on and names where it listens.
//!
//! The worker manifest carries the address as its two parts, [`ListenHost`]
//! and [`ListenPort`]; each has one bound and one parser, here.

use std::fmt;
use std::num::NonZeroU16;

use serde::{Deserialize, Serialize};

use crate::manifest_schema::{IntBound, ManifestValue, PythonValue};

/// The most bytes of a listen host: the longest DNS name (RFC 1035, 253
/// characters without the final dot). An IP address is shorter.
pub const MAX_LISTEN_HOST_BYTES: usize = 253;

/// The host of a listen address: a host name or an IP address, an IPv6
/// address without its brackets. 1 to [`MAX_LISTEN_HOST_BYTES`] bytes. Only
/// [`ListenHost::new`] builds it; the host is resolved when the endpoint
/// binds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ListenHost(String);

/// Why a text is not a listen host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListenHostRefused {
    /// The text is empty.
    Empty,
    /// The text has `bytes` bytes, more than [`MAX_LISTEN_HOST_BYTES`].
    TooLong { bytes: usize },
}

impl ListenHost {
    /// The host `text`. Pure.
    ///
    /// Errors: [`ListenHostRefused`].
    pub fn new(text: &str) -> Result<Self, ListenHostRefused> {
        match (text.is_empty(), text.len() > MAX_LISTEN_HOST_BYTES) {
            (true, _) => Err(ListenHostRefused::Empty),
            (false, true) => Err(ListenHostRefused::TooLong { bytes: text.len() }),
            (false, false) => Ok(Self(text.to_owned())),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ListenHost {
    type Error = String;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        Self::new(&text).map_err(|refused| match refused {
            ListenHostRefused::Empty => "a listen host must not be empty".to_owned(),
            ListenHostRefused::TooLong { bytes } => {
                format!("a listen host has at most {MAX_LISTEN_HOST_BYTES} bytes, not {bytes}")
            }
        })
    }
}

impl From<ListenHost> for String {
    fn from(host: ListenHost) -> Self {
        host.0
    }
}

impl ManifestValue for ListenHost {
    const PYTHON: PythonValue = PythonValue::Str;
}

/// The port of a listen address: 1 to 65535.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct ListenPort(NonZeroU16);

impl ListenPort {
    /// The port `port`, or `None` outside 1 to 65535. Pure.
    pub fn new(port: u32) -> Option<Self> {
        u16::try_from(port).ok().and_then(NonZeroU16::new).map(Self)
    }

    pub fn get(self) -> u16 {
        self.0.get()
    }
}

impl TryFrom<u32> for ListenPort {
    type Error = String;

    fn try_from(port: u32) -> Result<Self, Self::Error> {
        Self::new(port).ok_or_else(|| format!("a listen port must be from 1 to 65535, not {port}"))
    }
}

impl From<ListenPort> for u32 {
    fn from(port: ListenPort) -> Self {
        u32::from(port.get())
    }
}

impl ManifestValue for ListenPort {
    const PYTHON: PythonValue = PythonValue::Int(IntBound {
        name: "LISTEN_PORT",
        min: 1,
        max: u16::MAX as u64,
    });
}

/// The host and the port that a metrics endpoint binds. Built by
/// [`ListenAddress::parse`], or from its two checked parts
/// ([`ListenAddress::new`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListenAddress {
    host: ListenHost,
    port: ListenPort,
}

/// Why a text is not a `HOST:PORT` address. Each refusal names the text.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ListenAddressRefused {
    /// The text has no `:` before a port.
    #[error("{text:?} is not HOST:PORT")]
    NoPort { text: String },
    /// The text has nothing before the `:` of the port.
    #[error("{text:?} has no host before the port; give one, for example 127.0.0.1:9100")]
    NoHost { text: String },
    /// The host has more than [`MAX_LISTEN_HOST_BYTES`] bytes.
    #[error("the host of {text:?} has more than {MAX_LISTEN_HOST_BYTES} bytes")]
    HostTooLong { text: String },
    /// The text after the last `:` is not a number from 1 to 65535.
    #[error("the port of {text:?} is not a number from 1 to 65535")]
    PortOutOfRange { text: String },
    /// The host holds `:` and is not in brackets: an IPv6 address must be
    /// written `[address]:port`.
    #[error(
        "the host of {text:?} holds ':'; write an IPv6 address in brackets, for example \
         [::1]:9100"
    )]
    UnbracketedIpv6 { text: String },
}

impl ListenAddress {
    /// The address of `host` and `port`. Pure.
    pub fn new(host: ListenHost, port: ListenPort) -> Self {
        Self { host, port }
    }

    /// The address of `text`: `HOST:PORT`, or `[IPV6]:PORT`. Pure. The host
    /// is resolved when the endpoint binds, not here.
    ///
    /// Errors: [`ListenAddressRefused`], one variant per rule.
    pub fn parse(text: &str) -> Result<Self, ListenAddressRefused> {
        let refused_text = || text.to_owned();
        let Some((host_text, port_text)) = text.rsplit_once(':') else {
            return Err(ListenAddressRefused::NoPort {
                text: refused_text(),
            });
        };
        let Some(port) = port_number(port_text) else {
            return Err(ListenAddressRefused::PortOutOfRange {
                text: refused_text(),
            });
        };
        let host = match host_text
            .strip_prefix('[')
            .and_then(|inside| inside.strip_suffix(']'))
        {
            Some(bracketed) => bracketed,
            None if host_text.contains(':') => {
                return Err(ListenAddressRefused::UnbracketedIpv6 {
                    text: refused_text(),
                })
            }
            None => host_text,
        };
        match ListenHost::new(host) {
            Ok(host) => Ok(Self { host, port }),
            Err(ListenHostRefused::Empty) => Err(ListenAddressRefused::NoHost {
                text: refused_text(),
            }),
            Err(ListenHostRefused::TooLong { .. }) => Err(ListenAddressRefused::HostTooLong {
                text: refused_text(),
            }),
        }
    }

    /// The host: a name or an IP address, an IPv6 address without brackets.
    pub fn host(&self) -> &str {
        self.host.as_str()
    }

    /// The port, 1 to 65535.
    pub fn port(&self) -> u16 {
        self.port.get()
    }

    /// The checked host and port of this address, for the worker manifest.
    pub fn into_parts(self) -> (ListenHost, ListenPort) {
        (self.host, self.port)
    }
}

/// `HOST:PORT`, with an IPv6 host in brackets.
impl fmt::Display for ListenAddress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.host().contains(':') {
            true => write!(formatter, "[{}]:{}", self.host(), self.port()),
            false => write!(formatter, "{}:{}", self.host(), self.port()),
        }
    }
}

/// The port of `text`: ASCII digits only, 1 to 65535.
fn port_number(text: &str) -> Option<ListenPort> {
    match !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()) {
        true => text
            .parse::<u16>()
            .ok()
            .and_then(NonZeroU16::new)
            .map(ListenPort),
        false => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(text: &str) -> (String, u16) {
        let address = ListenAddress::parse(text).expect("a valid address");
        (address.host().to_owned(), address.port())
    }

    #[test]
    fn a_host_and_a_port_parse_and_print_back() {
        assert_eq!(parsed("127.0.0.1:9100"), ("127.0.0.1".to_owned(), 9100));
        assert_eq!(parsed("localhost:1"), ("localhost".to_owned(), 1));
        assert_eq!(parsed("0.0.0.0:65535"), ("0.0.0.0".to_owned(), 65535));
        assert_eq!(parsed("[::1]:9100"), ("::1".to_owned(), 9100));
        for text in ["127.0.0.1:9100", "[::1]:9100", "metrics.internal:8080"] {
            let address = ListenAddress::parse(text).expect("a valid address");
            assert_eq!(address.to_string(), text);
        }
    }

    #[test]
    fn a_text_without_a_port_is_refused() {
        for text in ["9100", "localhost", ""] {
            assert_eq!(
                ListenAddress::parse(text),
                Err(ListenAddressRefused::NoPort {
                    text: text.to_owned()
                })
            );
        }
    }

    #[test]
    fn a_text_without_a_host_is_refused() {
        for text in [":9100", "[]:9100"] {
            assert_eq!(
                ListenAddress::parse(text),
                Err(ListenAddressRefused::NoHost {
                    text: text.to_owned()
                })
            );
        }
    }

    #[test]
    fn a_port_outside_1_to_65535_or_not_digits_is_refused() {
        for text in [
            "localhost:0",
            "localhost:65536",
            "localhost:",
            "localhost:+80",
            "localhost:80a",
        ] {
            assert_eq!(
                ListenAddress::parse(text),
                Err(ListenAddressRefused::PortOutOfRange {
                    text: text.to_owned()
                })
            );
        }
    }

    #[test]
    fn a_host_over_the_byte_bound_is_refused() {
        let longest = format!("{}:9100", "a".repeat(MAX_LISTEN_HOST_BYTES));
        assert_eq!(parsed(&longest).0.len(), MAX_LISTEN_HOST_BYTES);
        let over = format!("{}:9100", "a".repeat(MAX_LISTEN_HOST_BYTES + 1));
        assert_eq!(
            ListenAddress::parse(&over),
            Err(ListenAddressRefused::HostTooLong { text: over.clone() })
        );
    }

    #[test]
    fn the_manifest_parts_read_their_bounds_and_build_the_address() {
        let host: ListenHost = serde_json::from_value(serde_json::json!("::1")).expect("a host");
        let port: ListenPort = serde_json::from_value(serde_json::json!(9100)).expect("a port");
        let address = ListenAddress::new(host.clone(), port);
        assert_eq!(address.to_string(), "[::1]:9100");
        assert_eq!(address.into_parts(), (host.clone(), port));
        assert_eq!(serde_json::to_value(&host).expect("a text"), "::1");
        assert_eq!(serde_json::to_value(port).expect("a number"), 9100);
        for refused in [
            serde_json::json!(""),
            serde_json::json!("a".repeat(MAX_LISTEN_HOST_BYTES + 1)),
        ] {
            assert!(serde_json::from_value::<ListenHost>(refused).is_err());
        }
        for refused in [
            serde_json::json!(0),
            serde_json::json!(65_536),
            serde_json::json!(-1),
        ] {
            assert!(serde_json::from_value::<ListenPort>(refused).is_err());
        }
        assert_eq!(ListenPort::new(1).map(ListenPort::get), Some(1));
        assert_eq!(ListenPort::new(65_535).map(ListenPort::get), Some(65_535));
    }

    #[test]
    fn an_ipv6_host_without_brackets_is_refused() {
        assert_eq!(
            ListenAddress::parse("::1:9100"),
            Err(ListenAddressRefused::UnbracketedIpv6 {
                text: "::1:9100".to_owned()
            })
        );
    }
}
