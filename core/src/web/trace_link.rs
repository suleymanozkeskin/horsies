//! The trace link of the monitoring page: an `http` or `https` URL template
//! with one `{trace_id}` placeholder, which the page fills with the 32 hex
//! digits of a task's trace id (for example
//! `https://grafana.example.com/explore?traceId={trace_id}`).
//!
//! The handler reports the template in `/api/meta`; the page builds the link
//! of each task detail. With no template, the page shows the trace id as
//! text.

/// The placeholder that the page replaces with the trace id.
pub const TRACE_ID_PLACEHOLDER: &str = "{trace_id}";

/// Most bytes of a template.
pub const MAX_TRACE_LINK_TEMPLATE_BYTES: usize = 2_048;

/// The schemes a template may start with.
const SCHEMES: [&str; 2] = ["https://", "http://"];

/// The trace link of the page.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum TraceLink {
    /// The page shows the trace id as text.
    #[default]
    Text,
    /// The page links the trace id with the template.
    Template(TraceLinkTemplate),
}

impl TraceLink {
    /// The template text that `/api/meta` reports, or `None`.
    pub fn template_text(&self) -> Option<&str> {
        match self {
            Self::Text => None,
            Self::Template(template) => Some(template.as_str()),
        }
    }
}

/// A template that passed [`TraceLinkTemplate::parse`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceLinkTemplate {
    text: String,
}

/// Why a text is not a trace link template. Each variant names the rule that
/// does not hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TraceLinkRefusal {
    #[error(
        "the template has {bytes} bytes; a template has 1 to {max} bytes",
        max = MAX_TRACE_LINK_TEMPLATE_BYTES
    )]
    Length { bytes: usize },
    #[error(
        "the template holds the byte 0x{byte:02x}; a template holds printable ASCII with no \
         space (0x21 to 0x7e)"
    )]
    Character { byte: u8 },
    #[error("the template does not start with https:// or http://")]
    Scheme,
    #[error("the template has no host after its scheme")]
    NoHost,
    #[error("the template holds {{trace_id}} {count} times; it holds it once")]
    PlaceholderCount { count: usize },
    #[error("the template holds a {{ or }} outside {{trace_id}}")]
    StrayBrace,
}

impl TraceLinkTemplate {
    /// The template of `text`, checked in this order: its length, its
    /// characters, its scheme, a host after the scheme, one placeholder, and
    /// no other brace. Pure; the work is bounded by the length check.
    pub fn parse(text: &str) -> Result<TraceLinkTemplate, TraceLinkRefusal> {
        let bytes = text.len();
        if bytes == 0 || bytes > MAX_TRACE_LINK_TEMPLATE_BYTES {
            return Err(TraceLinkRefusal::Length { bytes });
        }
        if let Some(byte) = text.bytes().find(|byte| !(0x21..=0x7e).contains(byte)) {
            return Err(TraceLinkRefusal::Character { byte });
        }
        let after_scheme = SCHEMES
            .iter()
            .find_map(|scheme| text.strip_prefix(scheme))
            .ok_or(TraceLinkRefusal::Scheme)?;
        let host_end = after_scheme
            .find(['/', '?', '#'])
            .unwrap_or(after_scheme.len());
        if host_end == 0 {
            return Err(TraceLinkRefusal::NoHost);
        }
        let count = text.matches(TRACE_ID_PLACEHOLDER).count();
        if count != 1 {
            return Err(TraceLinkRefusal::PlaceholderCount { count });
        }
        if text
            .replacen(TRACE_ID_PLACEHOLDER, "", 1)
            .contains(['{', '}'])
        {
            return Err(TraceLinkRefusal::StrayBrace);
        }
        Ok(TraceLinkTemplate {
            text: text.to_owned(),
        })
    }

    /// The template text.
    pub fn as_str(&self) -> &str {
        &self.text
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GRAFANA: &str = "https://grafana.example.com/explore?traceId={trace_id}";

    #[test]
    fn a_template_with_one_placeholder_after_a_host_is_accepted() {
        for text in [
            GRAFANA,
            "http://jaeger:16686/trace/{trace_id}",
            "https://example.com#/trace/{trace_id}",
        ] {
            let template = TraceLinkTemplate::parse(text).expect(text);
            assert_eq!(template.as_str(), text);
        }
    }

    #[test]
    fn a_template_outside_the_length_bound_is_refused() {
        assert_eq!(
            TraceLinkTemplate::parse(""),
            Err(TraceLinkRefusal::Length { bytes: 0 })
        );
        let long = format!(
            "https://example.com/{}{TRACE_ID_PLACEHOLDER}",
            "a".repeat(MAX_TRACE_LINK_TEMPLATE_BYTES)
        );
        assert_eq!(
            TraceLinkTemplate::parse(&long),
            Err(TraceLinkRefusal::Length { bytes: long.len() })
        );
    }

    #[test]
    fn a_space_or_a_control_or_non_ascii_byte_is_refused() {
        assert_eq!(
            TraceLinkTemplate::parse("https://example.com/a b/{trace_id}"),
            Err(TraceLinkRefusal::Character { byte: b' ' })
        );
        assert_eq!(
            TraceLinkTemplate::parse("https://example.com/\n{trace_id}"),
            Err(TraceLinkRefusal::Character { byte: b'\n' })
        );
        assert_eq!(
            TraceLinkTemplate::parse("https://exämple.com/{trace_id}"),
            Err(TraceLinkRefusal::Character { byte: 0xc3 })
        );
    }

    #[test]
    fn a_template_with_another_scheme_is_refused() {
        for text in [
            "ftp://example.com/{trace_id}",
            "javascript:alert('{trace_id}')",
            "HTTPS://example.com/{trace_id}",
            "/relative/{trace_id}",
        ] {
            assert_eq!(
                TraceLinkTemplate::parse(text),
                Err(TraceLinkRefusal::Scheme),
                "{text}"
            );
        }
    }

    #[test]
    fn a_template_with_no_host_is_refused() {
        assert_eq!(
            TraceLinkTemplate::parse("https:///trace/{trace_id}"),
            Err(TraceLinkRefusal::NoHost)
        );
    }

    #[test]
    fn a_template_holds_the_placeholder_once() {
        assert_eq!(
            TraceLinkTemplate::parse("https://example.com/trace"),
            Err(TraceLinkRefusal::PlaceholderCount { count: 0 })
        );
        assert_eq!(
            TraceLinkTemplate::parse("https://example.com/{trace_id}/{trace_id}"),
            Err(TraceLinkRefusal::PlaceholderCount { count: 2 })
        );
    }

    #[test]
    fn a_brace_outside_the_placeholder_is_refused() {
        assert_eq!(
            TraceLinkTemplate::parse("https://example.com/{span_id}/{trace_id}"),
            Err(TraceLinkRefusal::StrayBrace)
        );
        assert_eq!(
            TraceLinkTemplate::parse("https://example.com/{trace_id}}"),
            Err(TraceLinkRefusal::StrayBrace)
        );
    }

    #[test]
    fn the_meta_text_is_the_template_or_none() {
        let template = TraceLinkTemplate::parse(GRAFANA).expect("the template is valid");
        assert_eq!(TraceLink::Template(template).template_text(), Some(GRAFANA));
        assert_eq!(TraceLink::Text.template_text(), None);
    }
}
