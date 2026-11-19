//! `parse_listen_address`: the `HOST:PORT` address of a metrics endpoint by
//! the core's one parser, for `horsies worker --metrics-listen`.

use horsies_core::telemetry::listen::ListenAddress;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;

/// A parsed address: the host (an IPv6 address without brackets) and the
/// port, 1 to 65535.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct ListenAddressParsed {
    pub host: String,
    pub port: u16,
}

/// A text that is not a `HOST:PORT` address; `message` names the text and
/// the rule it breaks.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct ListenAddressRefused {
    pub message: String,
}

/// What `parse_listen_address` answers.
pub(crate) enum ListenParse {
    Parsed(ListenAddressParsed),
    Refused(ListenAddressRefused),
}

impl<'py> IntoPyObject<'py> for ListenParse {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Parsed(parsed) => parsed.into_bound_py_any(py),
            Self::Refused(refused) => refused.into_bound_py_any(py),
        }
    }
}

/// Parse `text` as `HOST:PORT` or `[IPV6]:PORT`: `ListenAddressParsed`, or
/// `ListenAddressRefused` with the rule it breaks (no port, no host, a host
/// over 253 bytes, a port outside 1 to 65535, an IPv6 host without
/// brackets). Pure: no I/O, and it starts no runtime; the host is resolved
/// when the endpoint binds.
#[pyfunction]
pub(crate) fn parse_listen_address(text: &str) -> ListenParse {
    match ListenAddress::parse(text) {
        Ok(address) => ListenParse::Parsed(ListenAddressParsed {
            host: address.host().to_owned(),
            port: address.port(),
        }),
        Err(refused) => ListenParse::Refused(ListenAddressRefused {
            message: refused.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_address_parses_into_its_host_and_port() {
        match parse_listen_address("[::1]:9100") {
            ListenParse::Parsed(parsed) => {
                assert_eq!((parsed.host.as_str(), parsed.port), ("::1", 9100));
            }
            ListenParse::Refused(refused) => panic!("a valid address: {}", refused.message),
        }
    }

    #[test]
    fn a_text_that_breaks_a_rule_is_refused_with_the_rule() {
        for (text, rule) in [
            ("9100", "is not HOST:PORT"),
            (":9100", "has no host before the port"),
            ("localhost:0", "is not a number from 1 to 65535"),
            ("::1:9100", "write an IPv6 address in brackets"),
        ] {
            match parse_listen_address(text) {
                ListenParse::Refused(refused) => {
                    assert!(
                        refused.message.contains(rule),
                        "{text}: {}",
                        refused.message
                    );
                }
                ListenParse::Parsed(parsed) => panic!("{text} parsed as {parsed:?}"),
            }
        }
    }
}
