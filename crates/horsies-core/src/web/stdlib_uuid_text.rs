//! A path id text read as Python's standard library reads it:
//! `uuid.UUID(text)`, the rule of `horsies.monitoring.get_workflow_run` and
//! `get_workflow_node`, which the Python web app's workflow read routes call
//! with the path text.
//!
//! The rule of CPython 3.13 and 3.14 (`Lib/uuid.py`):
//!
//! 1. Remove every `urn:`, then every `uuid:` (in lowercase only).
//! 2. Strip `{` and `}` from both ends, then remove every `-`.
//! 3. The rest must be 32 characters.
//! 4. `int(rest, 16)` reads the rest: ASCII whitespace around it, an
//!    optional `+`, an optional `0x` or `0X` with one optional `_` after it,
//!    and hex digits with single `_` between them. Before it reads, `int()`
//!    takes a whitespace character outside ASCII as a space and a decimal
//!    digit outside ASCII (Unicode category Nd) as its digit.
//! 5. The value must fit in 128 bits; it is the UUID.
//!
//! So `uuid:<id>`, `{<32 hex digits>}`, `12-34-<30 hex digits>` and
//! `0x<30 hex digits>` are read, and `URN:UUID:<id>` is refused.

use uuid::Uuid;

/// The prefixes that step 1 removes, in order.
const REMOVED_PREFIXES: [&str; 2] = ["urn:", "uuid:"];

/// The characters left after step 2 that a UUID text has.
const HEX_TEXT_CHARACTERS: usize = 32;

/// The first character that `int()` does not keep as it is: U+007F.
const FIRST_NOT_KEPT: u8 = 0x7f;

/// The zero of each run of ten decimal digits outside ASCII (Unicode
/// category Nd), in code point order: Unicode 15.1, the table of Python 3.13.
/// Python 3.14 (Unicode 16.0) adds the runs at U+10D40, U+116D0, U+116DA,
/// U+11BF0, U+16130, U+16D70, U+1CCF0 and U+1E5F1.
const UNICODE_DECIMAL_ZEROS: [u32; 67] = [
    0x0660, 0x06F0, 0x07C0, 0x0966, 0x09E6, 0x0A66, 0x0AE6, 0x0B66, 0x0BE6, 0x0C66, 0x0CE6, 0x0D66,
    0x0DE6, 0x0E50, 0x0ED0, 0x0F20, 0x1040, 0x1090, 0x17E0, 0x1810, 0x1946, 0x19D0, 0x1A80, 0x1A90,
    0x1B50, 0x1BB0, 0x1C40, 0x1C50, 0xA620, 0xA8D0, 0xA900, 0xA9D0, 0xA9F0, 0xAA50, 0xABF0, 0xFF10,
    0x104A0, 0x10D30, 0x11066, 0x110F0, 0x11136, 0x111D0, 0x112F0, 0x11450, 0x114D0, 0x11650,
    0x116C0, 0x11730, 0x118E0, 0x11950, 0x11C50, 0x11D50, 0x11DA0, 0x11F50, 0x16A60, 0x16AC0,
    0x16B50, 0x1D7CE, 0x1D7D8, 0x1D7E2, 0x1D7EC, 0x1D7F6, 0x1E140, 0x1E2F0, 0x1E4F0, 0x1E950,
    0x1FBF0,
];

/// The digits of one run.
const DIGITS_PER_RUN: u32 = 10;

/// Read `text` as Python's `uuid.UUID(text)` reads it. `None` for a text that
/// it refuses with `ValueError`. Pure; each step is one pass over the text.
pub(crate) fn parse_stdlib_uuid_text(text: &str) -> Option<Uuid> {
    let without_prefixes = REMOVED_PREFIXES
        .iter()
        .fold(text.to_owned(), |rest, prefix| rest.replace(prefix, ""));
    let hex_text: Vec<char> = without_prefixes
        .trim_matches(|character: char| character == '{' || character == '}')
        .chars()
        .filter(|character| *character != '-')
        .collect();
    if hex_text.len() != HEX_TEXT_CHARACTERS {
        return None;
    }
    let int_text = hex_text
        .into_iter()
        .map(int_text_byte)
        .collect::<Option<Vec<u8>>>()?;
    python_hex_int(&int_text).map(Uuid::from_u128)
}

/// The ASCII byte that `int()` reads for `character`: the character itself
/// below U+007F, a space for a whitespace character outside ASCII, the
/// digit of a decimal digit outside ASCII. `None` for any other character,
/// which `int()` refuses.
fn int_text_byte(character: char) -> Option<u8> {
    match u8::try_from(character) {
        Ok(byte) if byte < FIRST_NOT_KEPT => Some(byte),
        Ok(_) | Err(_) if character.is_whitespace() => Some(b' '),
        Ok(_) | Err(_) => unicode_decimal_digit(character).map(|digit| b'0' + digit),
    }
}

/// The digit of `character` when it is a decimal digit outside ASCII
/// ([`UNICODE_DECIMAL_ZEROS`]). Pure; a binary search of the table.
fn unicode_decimal_digit(character: char) -> Option<u8> {
    let code = u32::from(character);
    let run = UNICODE_DECIMAL_ZEROS
        .partition_point(|zero| *zero <= code)
        .checked_sub(1)?;
    let offset = code - UNICODE_DECIMAL_ZEROS.get(run)?;
    match offset < DIGITS_PER_RUN {
        true => u8::try_from(offset).ok(),
        false => None,
    }
}

/// The value that `int(text, 16)` reads from the ASCII bytes `text`
/// (CPython `PyLong_FromString`), if it fits in 128 bits. The text holds no
/// `-`: step 2 removed it. Pure.
fn python_hex_int(text: &[u8]) -> Option<u128> {
    let rest = without_leading_space(text);
    let rest = match rest {
        [b'+', after @ ..] => after,
        other => other,
    };
    let rest = match rest {
        [b'0', b'x' | b'X', b'_', after @ ..] | [b'0', b'x' | b'X', after @ ..] => after,
        other => other,
    };
    let (value, after_digits) = hex_digits(rest)?;
    match without_leading_space(after_digits) {
        [] => Some(value),
        [_, ..] => None,
    }
}

/// The hex digits at the start of `text`, with single `_` between them, and
/// the text after them. `None` when `text` starts with `_`, has no digit,
/// has two `_` in a row, or ends its digits with `_`, or when the value does
/// not fit in 128 bits. Pure; one pass.
fn hex_digits(text: &[u8]) -> Option<(u128, &[u8])> {
    let mut value: u128 = 0;
    let mut digits = 0_usize;
    let mut after_underscore = false;
    let mut end = text.len();
    for (index, byte) in text.iter().enumerate() {
        match (char::from(*byte).to_digit(16), *byte, after_underscore) {
            (Some(digit), _, _) => {
                value = value.checked_mul(16)?.checked_add(u128::from(digit))?;
                digits += 1;
                after_underscore = false;
            }
            (None, b'_', false) if index > 0 => after_underscore = true,
            (None, b'_', _) => return None,
            (None, _, _) => {
                end = index;
                break;
            }
        }
    }
    match (digits, after_underscore) {
        (0, _) | (_, true) => None,
        (_, false) => Some((value, &text[end..])),
    }
}

/// `text` without the ASCII whitespace at its start that `int()` skips:
/// space, tab, line feed, vertical tab, form feed, carriage return.
fn without_leading_space(text: &[u8]) -> &[u8] {
    let start = text
        .iter()
        .position(|byte| !matches!(*byte, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r'))
        .unwrap_or(text.len());
    &text[start..]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The UUID of the texts below: its first two hex digits are 0, so
    /// `int()` reads the same value with a prefix or a sign in their place.
    const ID: &str = "00345678-abcd-4f12-8456-7890abcdef12";

    fn expected() -> Option<Uuid> {
        Some(Uuid::from_u128(0x0034_5678_abcd_4f12_8456_7890_abcd_ef12))
    }

    /// Texts that Python 3.13's `uuid.UUID(text)` reads as `ID`.
    #[test]
    fn the_forms_that_python_reads_give_the_uuid() {
        for text in [
            ID.to_owned(),
            ID.to_uppercase(),
            format!("urn:uuid:{ID}"),
            format!("uuid:{ID}"),
            format!("urn:{ID}"),
            format!("{{{ID}}}"),
            "{{00345678abcd4f1284567890abcdef12}}}".to_owned(),
            "00345678abcd4f1284567890abcdef12".to_owned(),
            "00-34-5678abcd4f1284567890abcdef12".to_owned(),
            "0x345678abcd4f1284567890abcdef12".to_owned(),
            "{0x345678abcd4f1284567890abcdef12}".to_owned(),
            "+0345678abcd4f1284567890abcdef12".to_owned(),
            " 0345678abcd4f1284567890abcdef12".to_owned(),
            "\t\n345678abcd4f1284567890abcdef12".to_owned(),
            "0345678abcd4f1284567890abcdef12 ".to_owned(),
            "0345678abcd4f128_4567890abcdef12".to_owned(),
            "\u{00a0}0345678abcd4f1284567890abcdef12".to_owned(),
            "\u{3000}0345678abcd4f1284567890abcdef12".to_owned(),
            "\u{0660}0345678abcd4f1284567890abcdef12".to_owned(),
            "\u{1D7CE}\u{1D7D8}345678abcd4f1284567890abcdef12".to_owned(),
            "\u{FF10}\u{FF10}345678abcd4f1284567890abcdef12".to_owned(),
        ] {
            assert_eq!(parse_stdlib_uuid_text(&text), expected(), "{text:?}");
        }
    }

    /// Texts that Python 3.13's `uuid.UUID(text)` refuses with `ValueError`.
    #[test]
    fn the_forms_that_python_refuses_give_none() {
        for text in [
            format!("URN:UUID:{ID}"),
            format!("UUID:{ID}"),
            "u-uid:00345678abcd4f1284567890abcdef12".to_owned(),
            "not-a-uuid".to_owned(),
            String::new(),
            "00345678abcd4f1284567890abcdef1".to_owned(),
            "00345678abcd4f1284567890abcdef123".to_owned(),
            "_0345678abcd4f1284567890abcdef12".to_owned(),
            "0345678abcd4f1284567890abcdef12_".to_owned(),
            "0345678abcd4f12__4567890abcdef12".to_owned(),
            "0x__5678abcd4f1284567890abcdef12".to_owned(),
            "0x                              ".to_owned(),
            "+ 345678abcd4f1284567890abcdef12".to_owned(),
            "++345678abcd4f1284567890abcdef12".to_owned(),
            "\u{1c}0345678abcd4f1284567890abcdef12".to_owned(),
            "\u{7f}0345678abcd4f1284567890abcdef12".to_owned(),
            "\u{0}0345678abcd4f1284567890abcdef12".to_owned(),
            "0345678abcd4f1284567890abcdef1\u{0}2".to_owned(),
            "g0345678abcd4f1284567890abcdef12".to_owned(),
            "\u{00e9}0345678abcd4f1284567890abcdef12".to_owned(),
            "\u{10D40}0345678abcd4f1284567890abcdef12".to_owned(),
            "0345678abcd 4f1284567890abcdef12".to_owned(),
        ] {
            assert_eq!(parse_stdlib_uuid_text(&text), None, "{text:?}");
        }
    }

    #[test]
    fn the_largest_value_is_read_and_the_digits_of_each_run_count_from_its_zero() {
        assert_eq!(
            parse_stdlib_uuid_text("ffffffffffffffffffffffffffffffff"),
            Some(Uuid::from_u128(u128::MAX))
        );
        assert_eq!(
            parse_stdlib_uuid_text("0X_45678abcd4f1284567890abcdef12"),
            Some(Uuid::from_u128(0x4_5678_abcd_4f12_8456_7890_abcd_ef12))
        );
        assert_eq!(unicode_decimal_digit('\u{0669}'), Some(9));
        assert_eq!(unicode_decimal_digit('\u{066A}'), None);
        assert_eq!(unicode_decimal_digit('\u{1D7FF}'), Some(9));
        assert_eq!(unicode_decimal_digit('\u{065F}'), None);
    }

    #[test]
    fn the_zeros_are_in_order_and_their_runs_do_not_overlap() {
        for pair in UNICODE_DECIMAL_ZEROS.windows(2) {
            assert!(pair[0] + DIGITS_PER_RUN <= pair[1], "{pair:x?}");
        }
    }
}
