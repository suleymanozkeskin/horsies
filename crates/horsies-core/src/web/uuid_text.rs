//! A path id text read as pydantic reads a `uuid.UUID` (FastAPI's path
//! parameter in the Python web app): the forms that it accepts, and the
//! reason that it gives for any other text.
//!
//! The reasons are the texts of pydantic-core 2.46.4, the version in
//! `uv.lock`. They are computed here and not taken from `uuid::Error`: the
//! core's `uuid` release counts a character position from 0, words a length
//! refusal differently, and accepts `urn:uuid:` in any case, where
//! pydantic counts from 1 and accepts the prefix in lowercase only.

use std::fmt;

use uuid::Uuid;

/// The prefix of the URN form. pydantic accepts it in lowercase only.
const URN_PREFIX: &str = "urn:uuid:";

/// The byte length of the simple form: 32 hex digits.
const SIMPLE_LENGTH: usize = 32;

/// The byte length of the hyphenated form.
const HYPHENATED_LENGTH: usize = 36;

/// The byte length of the hyphenated form in braces.
const BRACED_LENGTH: usize = HYPHENATED_LENGTH + 2;

/// The byte length of the hyphenated form after [`URN_PREFIX`].
const URN_LENGTH: usize = URN_PREFIX.len() + HYPHENATED_LENGTH;

/// Where each group of the hyphenated form starts, in bytes.
const GROUP_STARTS: [usize; 5] = [0, 9, 14, 19, 24];

/// The length of each group of the hyphenated form, in hex digits.
const GROUP_LENGTHS: [usize; 5] = [8, 4, 4, 4, 12];

/// The number of `-` marks of the hyphenated form, which make its 5 groups.
const HYPHEN_COUNT: usize = 4;

/// The index of the last group of the hyphenated form.
const LAST_GROUP: usize = HYPHEN_COUNT;

/// Why a text is not a UUID, as pydantic words it. `Display` writes the
/// reason that follows `Input should be a valid UUID, ` in pydantic's
/// message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UuidTextRefusal {
    /// A character that is neither a hex digit nor `-`. The position is
    /// its byte offset in the read text plus 1.
    Character { character: char, position: usize },
    /// Text with no `-`, no braces and no [`URN_PREFIX`], whose byte length
    /// is not 32.
    SimpleLength { bytes: usize },
    /// Text whose `-` marks make a number of groups other than 5.
    GroupCount { groups: usize },
    /// Five groups, the first of which (in order) has the wrong length.
    GroupLength {
        group: usize,
        expected: usize,
        found: usize,
    },
}

impl fmt::Display for UuidTextRefusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Character {
                character,
                position,
            } => write!(
                formatter,
                "invalid character: found `{character}` at {position}"
            ),
            Self::SimpleLength { bytes } => write!(
                formatter,
                "invalid length: expected length 32 for simple format, found {bytes}"
            ),
            Self::GroupCount { groups } => {
                write!(formatter, "invalid group count: expected 5, found {groups}")
            }
            Self::GroupLength {
                group,
                expected,
                found,
            } => write!(
                formatter,
                "invalid group length in group {group}: expected {expected}, found {found}"
            ),
        }
    }
}

/// Read `text` as pydantic reads a `uuid.UUID`: 32 hex digits, the
/// hyphenated form, the hyphenated form in braces, or the hyphenated form
/// after a lowercase `urn:uuid:`. Hex digits may be in either case. Any
/// other text is refused with pydantic's reason. Pure.
pub(crate) fn parse_uuid_text(text: &str) -> Result<Uuid, UuidTextRefusal> {
    let candidate = match (text.len(), text.as_bytes()) {
        (SIMPLE_LENGTH | HYPHENATED_LENGTH, _) => text,
        (BRACED_LENGTH, [b'{', .., b'}']) => &text[1..BRACED_LENGTH - 1],
        (URN_LENGTH, bytes) if bytes.starts_with(URN_PREFIX.as_bytes()) => {
            &text[URN_PREFIX.len()..]
        }
        _ => return Err(refusal_of(text)),
    };
    // A candidate is 32 or 36 bytes long: the uuid crate reads it as the
    // simple or the hyphenated form, which is pydantic's rule for both.
    Uuid::try_parse(candidate).map_err(|_| refusal_of(candidate))
}

/// pydantic's reason for `text`, which is not a UUID. The text inside one
/// pair of braces, or after [`URN_PREFIX`], is read for its groups; a
/// character position counts the brace or the prefix. Pure; the loop ends
/// at the end of `text`.
fn refusal_of(text: &str) -> UuidTextRefusal {
    let (body, offset, plain) = match text.as_bytes() {
        [b'{', .., b'}'] => (&text[1..text.len() - 1], 1, false),
        bytes if bytes.starts_with(URN_PREFIX.as_bytes()) => {
            (&text[URN_PREFIX.len()..], URN_PREFIX.len(), false)
        }
        _ => (text, 0, true),
    };
    let mut hyphens = 0_usize;
    let mut hyphen_offsets = [0_usize; HYPHEN_COUNT];
    for (index, character) in body.char_indices() {
        match character {
            '-' => {
                if let Some(slot) = hyphen_offsets.get_mut(hyphens) {
                    *slot = index;
                }
                hyphens += 1;
            }
            digit if digit.is_ascii_hexdigit() => {}
            other => {
                return UuidTextRefusal::Character {
                    character: other,
                    position: index + offset + 1,
                }
            }
        }
    }
    match (hyphens, plain) {
        (0, true) => UuidTextRefusal::SimpleLength { bytes: text.len() },
        (HYPHEN_COUNT, _) => group_length_refusal(text.len(), hyphen_offsets),
        (other, _) => UuidTextRefusal::GroupCount { groups: other + 1 },
    }
}

/// The reason for five groups of which one has the wrong length: the first
/// of the four leading groups whose `-` is not where its length puts it,
/// else the last group, whose length pydantic counts as the byte length of
/// the whole read text (the brace or the prefix included) less 24. Pure.
fn group_length_refusal(
    text_bytes: usize,
    hyphen_offsets: [usize; HYPHEN_COUNT],
) -> UuidTextRefusal {
    for (group, hyphen) in hyphen_offsets.into_iter().enumerate() {
        let start = GROUP_STARTS[group];
        let expected = GROUP_LENGTHS[group];
        if hyphen != start + expected {
            // Each earlier group ended at its expected end, so this `-`
            // comes after `start`.
            return UuidTextRefusal::GroupLength {
                group,
                expected,
                found: hyphen - start,
            };
        }
    }
    UuidTextRefusal::GroupLength {
        group: LAST_GROUP,
        expected: GROUP_LENGTHS[LAST_GROUP],
        found: text_bytes - GROUP_STARTS[LAST_GROUP],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Texts that are not a UUID, with the reason of pydantic 2.13.4
    /// (pydantic-core 2.46.4): `TypeAdapter(UUID).validate_python(text)`,
    /// the message after `Input should be a valid UUID, `.
    const PYDANTIC_REASONS: &[(&str, &str)] = &[
        ("not-a-uuid", "invalid character: found `n` at 1"),
        ("0x", "invalid character: found `x` at 2"),
        (
            "",
            "invalid length: expected length 32 for simple format, found 0",
        ),
        (
            "123",
            "invalid length: expected length 32 for simple format, found 3",
        ),
        (
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "invalid length: expected length 32 for simple format, found 46",
        ),
        (
            "not-a-uuidnot-a-uuidnot-a-uuidnot-a-uuidnot-a-uuidnot-a-uuid",
            "invalid character: found `n` at 1",
        ),
        ("é", "invalid character: found `é` at 1"),
        ("aé", "invalid character: found `é` at 2"),
        ("\u{12d}", "invalid character: found `\u{12d}` at 1"),
        (
            "12345678-1234-1234-1234-12345678901é",
            "invalid character: found `é` at 36",
        ),
        (
            "{12345678-1234-1234-1234-12345678901g}",
            "invalid character: found `g` at 36",
        ),
        (
            "urn:uuid:12345678-1234-1234-1234-12345678901g",
            "invalid character: found `g` at 36",
        ),
        (
            "urn:uuid:{12345678-1234-1234-1234-123456789012}",
            "invalid character: found `{` at 10",
        ),
        (
            "URN:UUID:12345678-1234-1234-1234-123456789012",
            "invalid character: found `U` at 1",
        ),
        (
            "{12345678123412341234123456789012}",
            "invalid group count: expected 5, found 1",
        ),
        ("{}", "invalid group count: expected 5, found 1"),
        ("{", "invalid character: found `{` at 1"),
        ("-----", "invalid group count: expected 5, found 6"),
        ("1234-5678", "invalid group count: expected 5, found 2"),
        (
            "----",
            "invalid group length in group 0: expected 8, found 0",
        ),
        (
            "1-2-3-4-5",
            "invalid group length in group 0: expected 8, found 1",
        ),
        (
            "12345678_1234-1234-1234-123456789012",
            "invalid character: found `_` at 9",
        ),
        (
            "1234567-12345-1234-1234-123456789012",
            "invalid group length in group 0: expected 8, found 7",
        ),
        (
            "12345678-123-12345-1234-123456789012",
            "invalid group length in group 1: expected 4, found 3",
        ),
        (
            "12345678-1234-123-12345-123456789012",
            "invalid group length in group 2: expected 4, found 3",
        ),
        (
            "12345678-1234-1234-123-1234567890123",
            "invalid group length in group 3: expected 4, found 3",
        ),
        (
            "12345678-1234-1234-1234-1234567890120",
            "invalid group length in group 4: expected 12, found 13",
        ),
        (
            "{12345678-1234-1234-1234-1234567890}",
            "invalid group length in group 4: expected 12, found 12",
        ),
        (
            "urn:uuid:12345678-1234-1234-1234-1234567890120",
            "invalid group length in group 4: expected 12, found 22",
        ),
        (
            "{{2345678-1234-1234-1234-12345678901}}",
            "invalid group length in group 0: expected 8, found 7",
        ),
    ];

    /// Each text that pydantic refuses is refused with pydantic's reason.
    #[test]
    fn a_text_that_is_not_a_uuid_gets_pydantics_reason() {
        for (text, reason) in PYDANTIC_REASONS {
            let refusal = parse_uuid_text(text).expect_err(text);
            assert_eq!(refusal.to_string(), *reason, "{text:?}");
        }
    }

    /// The four forms that pydantic reads, hex digits in either case, give
    /// the same UUID.
    #[test]
    fn the_forms_that_pydantic_reads_give_the_uuid() {
        let expected = Uuid::from_u128(0x1234_5678_abcd_ef12_3456_7890_abcd_ef12);
        for text in [
            "12345678abcdef1234567890abcdef12",
            "12345678-abcd-ef12-3456-7890abcdef12",
            "12345678-ABCD-EF12-3456-7890ABCDEF12",
            "{12345678-abcd-ef12-3456-7890abcdef12}",
            "urn:uuid:12345678-abcd-ef12-3456-7890abcdef12",
        ] {
            assert_eq!(parse_uuid_text(text), Ok(expected), "{text:?}");
        }
    }

    /// Python's `uuid.UUID` reads a sign
    /// before 31 hex digits; a path id refuses it as a character that is not
    /// a hex digit.
    #[test]
    fn a_sign_before_the_hex_digits_is_refused() {
        let refusal = parse_uuid_text("+f8fad5bd9cb469fa16570867728950e")
            .expect_err("a sign is not a hex digit");
        assert_eq!(refusal.to_string(), "invalid character: found `+` at 1");
    }
}
