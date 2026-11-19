//! The address that a metrics endpoint binds, as a `--listen HOST:PORT`
//! argument gives it. A metrics endpoint has no default address: the
//! argument turns it on and names where it listens.

use std::fmt;
use std::num::NonZeroU16;

/// The host and the port that a metrics endpoint binds. Only
/// [`ListenAddress::parse`] builds it: the host is not empty, and the port is
/// 1 to 65535.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListenAddress {
    /// A host name or an IP address; an IPv6 address without its brackets.
    host: String,
    port: NonZeroU16,
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
        match host.is_empty() {
            true => Err(ListenAddressRefused::NoHost {
                text: refused_text(),
            }),
            false => Ok(Self {
                host: host.to_owned(),
                port,
            }),
        }
    }

    /// The host: a name or an IP address, an IPv6 address without brackets.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The port, 1 to 65535.
    pub fn port(&self) -> u16 {
        self.port.get()
    }
}

/// `HOST:PORT`, with an IPv6 host in brackets.
impl fmt::Display for ListenAddress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.host.contains(':') {
            true => write!(formatter, "[{}]:{}", self.host, self.port),
            false => write!(formatter, "{}:{}", self.host, self.port),
        }
    }
}

/// The port of `text`: ASCII digits only, 1 to 65535.
fn port_number(text: &str) -> Option<NonZeroU16> {
    match !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()) {
        true => text.parse::<u16>().ok().and_then(NonZeroU16::new),
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
    fn an_ipv6_host_without_brackets_is_refused() {
        assert_eq!(
            ListenAddress::parse("::1:9100"),
            Err(ListenAddressRefused::UnbracketedIpv6 {
                text: "::1:9100".to_owned()
            })
        );
    }
}
