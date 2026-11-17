//! Python's text of a stored value, for the messages that keep Python's
//! wording: a stored status, outcome or kind that does not parse is named
//! as Python's `repr` names it (`'NOT_KNOWN'`), not as Rust's `Debug` does
//! (`"NOT_KNOWN"`).

/// A text as Python's `repr` of a `str` shows it. Pure.
///
/// - The quote is `'`, or `"` when the text holds a `'` and no `"`.
/// - A backslash and the quote in use are escaped; tab, newline and carriage
///   return are written `\t`, `\n` and `\r`.
/// - Another control character (`U+0000` to `U+001F`, `U+007F` to `U+009F`)
///   and a whitespace character other than the space are written `\xNN` up
///   to `U+00FF`, else `\uNNNN`, in lowercase hex.
/// - Every other character is written as it is. Python also escapes the
///   format characters (Unicode category Cf), the private-use characters
///   (Co) and the code points that Unicode does not assign (Cn); this
///   function writes them as they are, because the core has no table of
///   Unicode categories.
pub fn python_repr(text: &str) -> String {
    let quote = match (text.contains('\''), text.contains('"')) {
        (true, false) => '"',
        (true, true) | (false, true) | (false, false) => '\'',
    };
    let mut shown = String::with_capacity(text.len() + 2);
    shown.push(quote);
    for character in text.chars() {
        match character {
            '\\' => shown.push_str("\\\\"),
            '\n' => shown.push_str("\\n"),
            '\r' => shown.push_str("\\r"),
            '\t' => shown.push_str("\\t"),
            other if other == quote => {
                shown.push('\\');
                shown.push(other);
            }
            other if other.is_control() || (other.is_whitespace() && other != ' ') => {
                shown.push_str(&hex_escape(other));
            }
            other => shown.push(other),
        }
    }
    shown.push(quote);
    shown
}

/// `\xNN` for a character up to `U+00FF`, else `\uNNNN`. Every character
/// that [`python_repr`] escapes this way is at most `U+3000`. Pure.
fn hex_escape(character: char) -> String {
    let code = u32::from(character);
    match u8::try_from(code) {
        Ok(byte) => format!("\\x{byte:02x}"),
        Err(_) => format!("\\u{code:04x}"),
    }
}

#[cfg(test)]
mod tests {
    use super::python_repr;

    #[test]
    fn a_plain_text_is_in_single_quotes() {
        assert_eq!(python_repr("BOGUS"), "'BOGUS'");
        assert_eq!(python_repr(""), "''");
    }

    #[test]
    fn a_text_with_a_single_quote_and_no_double_quote_is_in_double_quotes() {
        assert_eq!(python_repr("it's"), "\"it's\"");
    }

    #[test]
    fn a_text_with_both_quotes_escapes_the_single_quote() {
        assert_eq!(python_repr("a'b\"c"), "'a\\'b\"c'");
        assert_eq!(python_repr("\""), "'\"'");
    }

    #[test]
    fn a_backslash_tab_newline_and_carriage_return_are_escaped() {
        assert_eq!(python_repr("x\ny\\"), "'x\\ny\\\\'");
        assert_eq!(python_repr("a\tb\rc"), "'a\\tb\\rc'");
    }

    /// `repr('\x00\x1b\x7f\x85\x0b')` in Python.
    #[test]
    fn another_control_character_is_a_hex_escape() {
        assert_eq!(
            python_repr("\u{0}\u{1b}\u{7f}\u{85}\u{b}"),
            "'\\x00\\x1b\\x7f\\x85\\x0b'"
        );
    }

    /// `repr('a\xa0b c d')` in Python.
    #[test]
    fn a_whitespace_character_other_than_the_space_is_escaped() {
        assert_eq!(python_repr("a\u{a0}b\u{2028}c d"), "'a\\xa0b\\u2028c d'");
    }

    #[test]
    fn a_printable_character_outside_ascii_is_kept() {
        assert_eq!(python_repr("café 日本"), "'café 日本'");
    }
}
