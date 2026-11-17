//! The strict JSON read rule of the core (Python `core/codec/json_io.py`,
//! `loads_json` and `loads_json_bytes`).
//!
//! A text passes when Python's `loads_json` accepts it, with one decided
//! difference: an object that repeats a key is refused (Python's `json.loads`
//! keeps the last value). The rule:
//!
//! - RFC 8259 grammar, as Python's `json` module reads it: whitespace is
//!   space, tab, line feed and carriage return; a byte order mark is refused;
//!   one value, then only whitespace.
//! - No `NaN`, `Infinity` or `-Infinity`.
//! - An integer (no fraction, no exponent) has at most [`MAX_INT_DIGITS`]
//!   digits, Python's limit for integer text; it is kept exactly by readers.
//! - A number with a fraction or an exponent is a finite binary64 float:
//!   `1e400` is refused.
//! - A `\u` escape that leaves a lone UTF-16 surrogate is refused: UTF-8
//!   cannot store it.
//! - Nesting is at most [`MAX_JSON_DEPTH`] arrays and objects.
//! - An object repeats no key (keys compare after unescaping).
//! - Stored payloads are UTF-8 and not empty.
//!
//! A value text that user code built has two more rules ([`TextRule::UserValue`],
//! `super::user_value`): at most [`MAX_VALUE_JSON_DEPTH`] levels, and no key
//! that only the engine writes.
//!
//! The functions read the text and build no value. Error texts never name a
//! value of the text: the text is task input. Three refusals have the text of
//! Python's reader (`python/horsies/core/codec/json_value.py:156, 180, 294`),
//! so the public messages that carry them are the same in both clients: a
//! non-standard constant, a number outside finite binary64 and a lone
//! surrogate. Python's text for them names no byte offset, so neither does
//! the core's; the variant keeps the offset. The other refusals name the byte
//! offset where the text breaks the rule.

use std::collections::HashSet;

use super::user_value::{is_engine_key, MAX_VALUE_JSON_DEPTH};

/// The deepest nesting of arrays and objects that the boundary accepts
/// (Python `MAX_JSON_DEPTH`).
pub const MAX_JSON_DEPTH: usize = 300;

/// The most digits of an integer (Python `sys.get_int_max_str_digits()`
/// default). The sign does not count.
pub const MAX_INT_DIGITS: usize = 4300;

/// Why a JSON text is refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StrictJsonError {
    /// The text or payload is empty.
    #[error("JSON text is empty")]
    Empty,
    /// The payload is not UTF-8 from this byte on.
    #[error("JSON payload is not UTF-8 at byte {offset}")]
    NotUtf8 { offset: usize },
    /// The text breaks the JSON grammar at this byte.
    #[error("JSON parse failed at byte {offset}: {reason}")]
    Syntax { offset: usize, reason: SyntaxReason },
    /// `NaN`, `Infinity` or `-Infinity` at this byte. Python's text
    /// (`_reject_nonstandard_json_constant`).
    #[error("Non-standard JSON constant is not allowed: {constant}")]
    NonStandardConstant {
        offset: usize,
        constant: JsonConstant,
    },
    /// An integer with more than [`MAX_INT_DIGITS`] digits.
    #[error("JSON integer at byte {offset} has more than {MAX_INT_DIGITS} digits")]
    IntegerTooLong { offset: usize },
    /// A float that binary64 cannot hold as a finite value, starting at this
    /// byte. Python's text (`_parse_finite_float`).
    #[error("JSON number is outside the finite binary64 range")]
    NonFiniteFloat { offset: usize },
    /// A `\u` escape that leaves a lone UTF-16 surrogate, in the string that
    /// starts at this byte. Python's text (`_reject_lone_surrogate`).
    #[error("JSON string holds a lone surrogate")]
    LoneSurrogate { offset: usize },
    /// Arrays and objects nested deeper than the bound of the text kind:
    /// [`MAX_JSON_DEPTH`] for a stored text, [`MAX_VALUE_JSON_DEPTH`] for a
    /// user value.
    #[error("JSON text is nested deeper than {limit} levels")]
    TooDeep { limit: usize },
    /// A user value holds a key that only the engine writes; the key starts at
    /// this byte.
    #[error("JSON object holds a reserved engine key at byte {offset}")]
    EngineKey { offset: usize },
    /// An object repeats the key that starts at this byte.
    #[error("JSON object repeats the key at byte {offset}")]
    DuplicateKey { offset: usize },
}

/// A constant that Python's `json.loads` reads by default and the strict rule
/// refuses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonConstant {
    Nan,
    Infinity,
    NegativeInfinity,
}

impl JsonConstant {
    /// The constant as a JSON text writes it, which is the name that
    /// Python's `parse_constant` hook gets.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Nan => "NaN",
            Self::Infinity => "Infinity",
            Self::NegativeInfinity => "-Infinity",
        }
    }
}

impl std::fmt::Display for JsonConstant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The grammar rule that a refused text breaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyntaxReason {
    UnexpectedEnd,
    UnexpectedCharacter,
    ControlCharacterInString,
    InvalidEscape,
    InvalidNumber,
    TrailingData,
}

impl std::fmt::Display for SyntaxReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            Self::UnexpectedEnd => "the text ends inside a value",
            Self::UnexpectedCharacter => "a character that cannot start or continue a value",
            Self::ControlCharacterInString => "a control character inside a string",
            Self::InvalidEscape => "an invalid escape in a string",
            Self::InvalidNumber => "an invalid number",
            Self::TrailingData => "data after the value",
        };
        f.write_str(text)
    }
}

/// Which text the reader checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextRule {
    /// A stored text: at most [`MAX_JSON_DEPTH`] levels; any key.
    Stored,
    /// A value that user code built: at most [`MAX_VALUE_JSON_DEPTH`] levels;
    /// no key that only the engine writes.
    UserValue,
}

impl TextRule {
    /// The deepest nesting that the rule accepts.
    pub fn max_depth(self) -> usize {
        match self {
            Self::Stored => MAX_JSON_DEPTH,
            Self::UserValue => MAX_VALUE_JSON_DEPTH,
        }
    }
}

/// Check a stored payload: not empty, UTF-8, then [`check_json_text`].
pub fn check_json_bytes(payload: &[u8]) -> Result<(), StrictJsonError> {
    check_json_bytes_with(payload, TextRule::Stored)
}

/// Check a JSON text against the strict read rule (module docs). Does no
/// I/O and builds no value.
pub fn check_json_text(text: &str) -> Result<(), StrictJsonError> {
    check_json_text_with(text, TextRule::Stored)
}

/// Check a payload under `rule`: not empty, UTF-8, then [`check_json_text_with`].
pub fn check_json_bytes_with(payload: &[u8], rule: TextRule) -> Result<(), StrictJsonError> {
    if payload.is_empty() {
        return Err(StrictJsonError::Empty);
    }
    let text = std::str::from_utf8(payload).map_err(|error| StrictJsonError::NotUtf8 {
        offset: error.valid_up_to(),
    })?;
    check_json_text_with(text, rule)
}

/// Check a JSON text against the strict read rule and the depth and key rules
/// of `rule`. Does no I/O and builds no value.
pub fn check_json_text_with(text: &str, rule: TextRule) -> Result<(), StrictJsonError> {
    if text.is_empty() {
        return Err(StrictJsonError::Empty);
    }
    let mut reader = Reader {
        bytes: text.as_bytes(),
        pos: 0,
        rule,
    };
    reader.skip_whitespace();
    reader.value(0)?;
    reader.skip_whitespace();
    match reader.pos == reader.bytes.len() {
        true => Ok(()),
        false => Err(reader.syntax(SyntaxReason::TrailingData)),
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
    rule: TextRule,
}

impl Reader<'_> {
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn syntax(&self, reason: SyntaxReason) -> StrictJsonError {
        StrictJsonError::Syntax {
            offset: self.pos,
            reason,
        }
    }

    fn non_standard_constant(&self, constant: JsonConstant) -> StrictJsonError {
        StrictJsonError::NonStandardConstant {
            offset: self.pos,
            constant,
        }
    }

    fn rest_starts_with(&self, literal: &[u8]) -> bool {
        self.bytes[self.pos..].starts_with(literal)
    }

    fn skip_whitespace(&mut self) {
        while let Some(b' ' | b'\t' | b'\n' | b'\r') = self.peek() {
            self.pos += 1;
        }
    }

    /// One value. `depth` is the count of arrays and objects around it.
    fn value(&mut self, depth: usize) -> Result<(), StrictJsonError> {
        match self.peek() {
            None => Err(self.syntax(SyntaxReason::UnexpectedEnd)),
            Some(b'{') => self.object(depth + 1),
            Some(b'[') => self.array(depth + 1),
            Some(b'"') => self.string().map(|_key| ()),
            Some(b't') => self.literal(b"true"),
            Some(b'f') => self.literal(b"false"),
            Some(b'n') => self.literal(b"null"),
            Some(b'N') if self.rest_starts_with(b"NaN") => {
                Err(self.non_standard_constant(JsonConstant::Nan))
            }
            Some(b'I') if self.rest_starts_with(b"Infinity") => {
                Err(self.non_standard_constant(JsonConstant::Infinity))
            }
            Some(b'-') if self.rest_starts_with(b"-Infinity") => {
                Err(self.non_standard_constant(JsonConstant::NegativeInfinity))
            }
            Some(b'-' | b'0'..=b'9') => self.number(),
            Some(_) => Err(self.syntax(SyntaxReason::UnexpectedCharacter)),
        }
    }

    fn literal(&mut self, literal: &[u8]) -> Result<(), StrictJsonError> {
        match self.rest_starts_with(literal) {
            true => {
                self.pos += literal.len();
                Ok(())
            }
            false => Err(self.syntax(SyntaxReason::UnexpectedCharacter)),
        }
    }

    fn object(&mut self, depth: usize) -> Result<(), StrictJsonError> {
        if depth > self.rule.max_depth() {
            return Err(StrictJsonError::TooDeep {
                limit: self.rule.max_depth(),
            });
        }
        self.pos += 1; // '{'
        self.skip_whitespace();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            return Ok(());
        }
        let mut keys: HashSet<String> = HashSet::new();
        loop {
            let key_offset = self.pos;
            match self.peek() {
                Some(b'"') => {}
                None => return Err(self.syntax(SyntaxReason::UnexpectedEnd)),
                Some(_) => return Err(self.syntax(SyntaxReason::UnexpectedCharacter)),
            }
            let key = self.string()?;
            if self.rule == TextRule::UserValue && is_engine_key(&key) {
                return Err(StrictJsonError::EngineKey { offset: key_offset });
            }
            if !keys.insert(key) {
                return Err(StrictJsonError::DuplicateKey { offset: key_offset });
            }
            self.skip_whitespace();
            match self.peek() {
                Some(b':') => self.pos += 1,
                None => return Err(self.syntax(SyntaxReason::UnexpectedEnd)),
                Some(_) => return Err(self.syntax(SyntaxReason::UnexpectedCharacter)),
            }
            self.skip_whitespace();
            self.value(depth)?;
            self.skip_whitespace();
            match self.peek() {
                Some(b',') => {
                    self.pos += 1;
                    self.skip_whitespace();
                }
                Some(b'}') => {
                    self.pos += 1;
                    return Ok(());
                }
                None => return Err(self.syntax(SyntaxReason::UnexpectedEnd)),
                Some(_) => return Err(self.syntax(SyntaxReason::UnexpectedCharacter)),
            }
        }
    }

    fn array(&mut self, depth: usize) -> Result<(), StrictJsonError> {
        if depth > self.rule.max_depth() {
            return Err(StrictJsonError::TooDeep {
                limit: self.rule.max_depth(),
            });
        }
        self.pos += 1; // '['
        self.skip_whitespace();
        if self.peek() == Some(b']') {
            self.pos += 1;
            return Ok(());
        }
        loop {
            self.value(depth)?;
            self.skip_whitespace();
            match self.peek() {
                Some(b',') => {
                    self.pos += 1;
                    self.skip_whitespace();
                }
                Some(b']') => {
                    self.pos += 1;
                    return Ok(());
                }
                None => return Err(self.syntax(SyntaxReason::UnexpectedEnd)),
                Some(_) => return Err(self.syntax(SyntaxReason::UnexpectedCharacter)),
            }
        }
    }

    /// A string at the current `"`; returns its unescaped text (object keys
    /// compare by it).
    fn string(&mut self) -> Result<String, StrictJsonError> {
        let start = self.pos;
        self.pos += 1; // '"'
        let mut text: Vec<u8> = Vec::new();
        loop {
            match self.peek() {
                None => return Err(self.syntax(SyntaxReason::UnexpectedEnd)),
                Some(b'"') => {
                    self.pos += 1;
                    break;
                }
                Some(b'\\') => {
                    self.pos += 1;
                    self.escape(start, &mut text)?;
                }
                Some(byte) if byte < 0x20 => {
                    return Err(self.syntax(SyntaxReason::ControlCharacterInString))
                }
                Some(byte) => {
                    text.push(byte);
                    self.pos += 1;
                }
            }
        }
        // The raw bytes come from a `&str` and the escapes push whole
        // characters, so the text is UTF-8.
        String::from_utf8(text).map_err(|_| StrictJsonError::Syntax {
            offset: start,
            reason: SyntaxReason::InvalidEscape,
        })
    }

    /// One escape after its `\`; pushes the character it names.
    fn escape(&mut self, string_start: usize, text: &mut Vec<u8>) -> Result<(), StrictJsonError> {
        let simple = match self.peek() {
            None => return Err(self.syntax(SyntaxReason::UnexpectedEnd)),
            Some(b'"') => Some('"'),
            Some(b'\\') => Some('\\'),
            Some(b'/') => Some('/'),
            Some(b'b') => Some('\u{8}'),
            Some(b'f') => Some('\u{c}'),
            Some(b'n') => Some('\n'),
            Some(b'r') => Some('\r'),
            Some(b't') => Some('\t'),
            Some(b'u') => None,
            Some(_) => return Err(self.syntax(SyntaxReason::InvalidEscape)),
        };
        self.pos += 1;
        let character = match simple {
            Some(character) => character,
            None => self.unicode_escape(string_start)?,
        };
        let mut buffer = [0u8; 4];
        text.extend_from_slice(character.encode_utf8(&mut buffer).as_bytes());
        Ok(())
    }

    /// The character of a `\uXXXX` escape (after the `u`), joining a
    /// surrogate pair. A surrogate without its pair is refused.
    fn unicode_escape(&mut self, string_start: usize) -> Result<char, StrictJsonError> {
        let lone = StrictJsonError::LoneSurrogate {
            offset: string_start,
        };
        let first = self.hex4()?;
        let code_point = match first {
            0xD800..=0xDBFF => {
                if !self.rest_starts_with(b"\\u") {
                    return Err(lone);
                }
                self.pos += 2;
                let second = self.hex4()?;
                match second {
                    0xDC00..=0xDFFF => {
                        0x1_0000
                            + ((u32::from(first) - 0xD800) << 10)
                            + (u32::from(second) - 0xDC00)
                    }
                    _ => return Err(lone),
                }
            }
            0xDC00..=0xDFFF => return Err(lone),
            other => u32::from(other),
        };
        char::from_u32(code_point).ok_or(lone)
    }

    fn hex4(&mut self) -> Result<u16, StrictJsonError> {
        let digits = self
            .bytes
            .get(self.pos..self.pos + 4)
            .ok_or_else(|| self.syntax(SyntaxReason::InvalidEscape))?;
        let text =
            std::str::from_utf8(digits).map_err(|_| self.syntax(SyntaxReason::InvalidEscape))?;
        if !text.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(self.syntax(SyntaxReason::InvalidEscape));
        }
        let value =
            u16::from_str_radix(text, 16).map_err(|_| self.syntax(SyntaxReason::InvalidEscape))?;
        self.pos += 4;
        Ok(value)
    }

    fn digits(&mut self) -> usize {
        let start = self.pos;
        while let Some(b'0'..=b'9') = self.peek() {
            self.pos += 1;
        }
        self.pos - start
    }

    /// A number: `-? (0 | [1-9][0-9]*) (. [0-9]+)? ([eE] [+-]? [0-9]+)?`.
    fn number(&mut self) -> Result<(), StrictJsonError> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        let integer_digits = match self.peek() {
            Some(b'0') => {
                self.pos += 1;
                1
            }
            Some(b'1'..=b'9') => self.digits(),
            None => return Err(self.syntax(SyntaxReason::UnexpectedEnd)),
            Some(_) => return Err(self.syntax(SyntaxReason::InvalidNumber)),
        };
        let mut is_float = false;
        if self.peek() == Some(b'.') && matches!(self.bytes.get(self.pos + 1), Some(b'0'..=b'9')) {
            self.pos += 1;
            let _fraction_digits = self.digits();
            is_float = true;
        }
        if let Some(b'e' | b'E') = self.peek() {
            let mark = self.pos;
            self.pos += 1;
            if let Some(b'+' | b'-') = self.peek() {
                self.pos += 1;
            }
            match self.digits() {
                0 => self.pos = mark, // `1e` reads as `1` followed by data
                _ => is_float = true,
            }
        }
        match is_float {
            false if integer_digits > MAX_INT_DIGITS => {
                Err(StrictJsonError::IntegerTooLong { offset: start })
            }
            false => Ok(()),
            true => {
                let literal = std::str::from_utf8(&self.bytes[start..self.pos])
                    .map_err(|_| self.syntax(SyntaxReason::InvalidNumber))?;
                let value: f64 = literal
                    .parse()
                    .map_err(|_| self.syntax(SyntaxReason::InvalidNumber))?;
                match value.is_finite() {
                    true => Ok(()),
                    false => Err(StrictJsonError::NonFiniteFloat { offset: start }),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What Python 0.7's `loads_json` does with a text, and what the core
    /// does. They differ only where the owner decided (duplicate keys).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Expected {
        Accept,
        Refuse,
        /// Python's `json.loads` keeps the last value; the core refuses.
        RefuseDecidedDuplicateKey,
    }

    /// The differential list: texts and Python 0.7 `loads_json` verdicts.
    const DIFFERENTIAL: &[(&str, Expected)] = &[
        // Scalars and containers that Python accepts.
        ("null", Expected::Accept),
        ("true", Expected::Accept),
        ("false", Expected::Accept),
        ("0", Expected::Accept),
        ("-0", Expected::Accept),
        ("1.5", Expected::Accept),
        ("-1.5e-3", Expected::Accept),
        ("1E5", Expected::Accept),
        ("1e+5", Expected::Accept),
        ("1e-400", Expected::Accept),
        ("123456789012345678901234567890", Expected::Accept),
        ("\"\"", Expected::Accept),
        ("\"a\\\"b\\\\c\\/d\\b\\f\\n\\r\\t\"", Expected::Accept),
        ("\"\\u00e9\\u20ac\"", Expected::Accept),
        ("\"\\ud83d\\ude00\"", Expected::Accept),
        ("\"\\uD83D\\uDE00\"", Expected::Accept),
        ("\"caf\u{e9} \u{1f600}\"", Expected::Accept),
        ("\"\u{7f}\"", Expected::Accept),
        ("[]", Expected::Accept),
        ("{}", Expected::Accept),
        (" \t\r\n[1, 2 ,3] \n", Expected::Accept),
        ("{\"a\": {\"b\": [1, {\"c\": null}]}}", Expected::Accept),
        ("{\"a\": 1, \"A\": 2}", Expected::Accept),
        // Python's json.loads accepts these, and loads_json refuses them.
        ("NaN", Expected::Refuse),
        ("Infinity", Expected::Refuse),
        ("-Infinity", Expected::Refuse),
        ("[NaN]", Expected::Refuse),
        ("1e400", Expected::Refuse),
        ("-1e400", Expected::Refuse),
        ("\"\\ud800\"", Expected::Refuse),
        ("\"\\udc00\"", Expected::Refuse),
        ("\"\\ud800\\u0041\"", Expected::Refuse),
        ("\"\\ud800x\"", Expected::Refuse),
        // Python's json.loads refuses these.
        ("", Expected::Refuse),
        ("   ", Expected::Refuse),
        ("\u{feff}{}", Expected::Refuse),
        ("01", Expected::Refuse),
        ("1.", Expected::Refuse),
        (".5", Expected::Refuse),
        ("+1", Expected::Refuse),
        ("1e", Expected::Refuse),
        ("-", Expected::Refuse),
        ("0x10", Expected::Refuse),
        ("[1,]", Expected::Refuse),
        ("{\"a\":1,}", Expected::Refuse),
        ("[1 2]", Expected::Refuse),
        ("{a: 1}", Expected::Refuse),
        ("{'a': 1}", Expected::Refuse),
        ("{\"a\" 1}", Expected::Refuse),
        ("{1: 2}", Expected::Refuse),
        ("[1] [2]", Expected::Refuse),
        ("True", Expected::Refuse),
        ("nul", Expected::Refuse),
        ("\"abc", Expected::Refuse),
        ("\"a\nb\"", Expected::Refuse),
        ("\"a\tb\"", Expected::Refuse),
        ("\"\\x41\"", Expected::Refuse),
        ("\"\\u12\"", Expected::Refuse),
        ("\"\\u12G4\"", Expected::Refuse),
        ("\u{a0}1", Expected::Refuse),
        ("[", Expected::Refuse),
        ("{\"a\":", Expected::Refuse),
        ("/* comment */ 1", Expected::Refuse),
        // Duplicate keys: decided refusal.
        ("{\"a\": 1, \"a\": 2}", Expected::RefuseDecidedDuplicateKey),
        (
            "{\"a\": 1, \"\\u0061\": 2}",
            Expected::RefuseDecidedDuplicateKey,
        ),
        (
            "[{\"k\": [], \"k\": {}}]",
            Expected::RefuseDecidedDuplicateKey,
        ),
    ];

    #[test]
    fn the_differential_list_matches_python_and_the_decided_key_rule() {
        for (text, expected) in DIFFERENTIAL {
            let outcome = check_json_text(text);
            match expected {
                Expected::Accept => assert_eq!(outcome, Ok(()), "{text:?}"),
                Expected::Refuse => assert!(
                    outcome.is_err()
                        && !matches!(outcome, Err(StrictJsonError::DuplicateKey { .. })),
                    "{text:?}: {outcome:?}",
                ),
                Expected::RefuseDecidedDuplicateKey => assert!(
                    matches!(outcome, Err(StrictJsonError::DuplicateKey { .. })),
                    "{text:?}: {outcome:?}",
                ),
            }
        }
    }

    #[test]
    fn each_refusal_names_its_rule() {
        let cases: &[(&str, StrictJsonError)] = &[
            ("", StrictJsonError::Empty),
            (
                "[NaN]",
                StrictJsonError::NonStandardConstant {
                    offset: 1,
                    constant: JsonConstant::Nan,
                },
            ),
            (
                "{\"a\":Infinity}",
                StrictJsonError::NonStandardConstant {
                    offset: 5,
                    constant: JsonConstant::Infinity,
                },
            ),
            (
                "[1,-Infinity]",
                StrictJsonError::NonStandardConstant {
                    offset: 3,
                    constant: JsonConstant::NegativeInfinity,
                },
            ),
            ("[1e400]", StrictJsonError::NonFiniteFloat { offset: 1 }),
            ("\"\\ud800\"", StrictJsonError::LoneSurrogate { offset: 0 }),
            (
                "{\"a\":1,\"a\":2}",
                StrictJsonError::DuplicateKey { offset: 7 },
            ),
            (
                "[1] x",
                StrictJsonError::Syntax {
                    offset: 4,
                    reason: SyntaxReason::TrailingData,
                },
            ),
            (
                "\"a\u{1}\"",
                StrictJsonError::Syntax {
                    offset: 2,
                    reason: SyntaxReason::ControlCharacterInString,
                },
            ),
        ];
        for (text, expected) in cases {
            assert_eq!(check_json_text(text).as_ref(), Err(expected), "{text:?}");
        }
    }

    /// The refusals that Python's reader words itself have Python's text
    /// (`python/horsies/core/codec/json_value.py:156, 180, 294`), with no
    /// byte offset, as Python's text has none.
    #[test]
    fn the_refusals_that_python_words_have_python_s_text() {
        let cases: &[(&str, &str)] = &[
            (
                "{\"x\":NaN}",
                "Non-standard JSON constant is not allowed: NaN",
            ),
            (
                "{\"x\":Infinity}",
                "Non-standard JSON constant is not allowed: Infinity",
            ),
            (
                "[-Infinity]",
                "Non-standard JSON constant is not allowed: -Infinity",
            ),
            (
                "{\"x\":1e9999}",
                "JSON number is outside the finite binary64 range",
            ),
            ("-1e400", "JSON number is outside the finite binary64 range"),
            ("{\"x\":\"\\ud800\"}", "JSON string holds a lone surrogate"),
            ("\"\\udc00\"", "JSON string holds a lone surrogate"),
        ];
        for (text, expected) in cases {
            let refused = check_json_text(text).expect_err(text);
            assert_eq!(refused.to_string(), *expected, "{text:?}");
        }
    }

    /// Python's integer text limit: 4300 digits pass, 4301 do not; the sign
    /// does not count. A long float literal has no digit limit.
    #[test]
    fn integers_follow_pythons_digit_limit() {
        let at_limit = "9".repeat(MAX_INT_DIGITS);
        assert_eq!(check_json_text(&at_limit), Ok(()));
        assert_eq!(check_json_text(&format!("-{at_limit}")), Ok(()));
        assert_eq!(check_json_text(&format!("[{at_limit}]")), Ok(()));

        let over = "9".repeat(MAX_INT_DIGITS + 1);
        assert_eq!(
            check_json_text(&over),
            Err(StrictJsonError::IntegerTooLong { offset: 0 })
        );
        assert_eq!(
            check_json_text(&format!("[-{over}]")),
            Err(StrictJsonError::IntegerTooLong { offset: 1 })
        );

        let small_long_float = format!("0.{}1", "0".repeat(MAX_INT_DIGITS * 2));
        assert_eq!(check_json_text(&small_long_float), Ok(()));
        let huge_float = format!("{over}.0");
        assert_eq!(
            check_json_text(&huge_float),
            Err(StrictJsonError::NonFiniteFloat { offset: 0 })
        );
    }

    /// Python `MAX_JSON_DEPTH`: 300 nested containers pass, 301 do not.
    #[test]
    fn nesting_is_bounded_at_300() {
        let nested = |depth: usize| format!("{}{}", "[".repeat(depth), "]".repeat(depth));
        assert_eq!(check_json_text(&nested(MAX_JSON_DEPTH)), Ok(()));
        let too_deep = Err(StrictJsonError::TooDeep {
            limit: MAX_JSON_DEPTH,
        });
        assert_eq!(check_json_text(&nested(MAX_JSON_DEPTH + 1)), too_deep);
        let objects = format!(
            "{}1{}",
            "{\"k\":".repeat(MAX_JSON_DEPTH + 1),
            "}".repeat(MAX_JSON_DEPTH + 1)
        );
        assert_eq!(check_json_text(&objects), too_deep);
        // A text far deeper than the bound is refused without deep recursion.
        assert_eq!(check_json_text(&"[".repeat(1_000_000)), too_deep);
    }

    /// Python `MAX_TYPED_JSON_DEPTH`: a user value of 200 levels passes, 201
    /// do not; a stored text keeps room for the envelopes around it.
    #[test]
    fn a_user_value_is_bounded_at_200() {
        let nested = |depth: usize| format!("{}{}", "[".repeat(depth), "]".repeat(depth));
        assert_eq!(
            check_json_text_with(&nested(MAX_VALUE_JSON_DEPTH), TextRule::UserValue),
            Ok(())
        );
        assert_eq!(
            check_json_text_with(&nested(MAX_VALUE_JSON_DEPTH + 1), TextRule::UserValue),
            Err(StrictJsonError::TooDeep {
                limit: MAX_VALUE_JSON_DEPTH
            })
        );
        assert_eq!(check_json_text(&nested(MAX_VALUE_JSON_DEPTH + 1)), Ok(()));
    }

    /// Python `_scan_reserved_keys`: a user value with an engine key at any
    /// depth is refused; a stored text may hold one.
    #[test]
    fn a_user_value_holds_no_engine_key() {
        for (text, offset) in [
            ("{\"__h_task_result__\":true}", 1),
            ("[{\"a\":{\"__builtin_task_code__\":\"X\"}}]", 7),
            ("{\"\\u005f_horsies_x\":1}", 1),
        ] {
            assert_eq!(
                check_json_text_with(text, TextRule::UserValue),
                Err(StrictJsonError::EngineKey { offset }),
                "{text}"
            );
            assert_eq!(check_json_text(text), Ok(()), "{text}");
        }
        assert_eq!(
            check_json_text_with("{\"note\":\"__h_task_result__\"}", TextRule::UserValue),
            Ok(())
        );
    }

    /// Python `loads_json_bytes`: empty bytes and bytes that are not UTF-8
    /// are refused; the error names the byte, not the value.
    #[test]
    fn stored_payloads_are_utf8_and_not_empty() {
        assert_eq!(check_json_bytes(b""), Err(StrictJsonError::Empty));
        assert_eq!(
            check_json_bytes(b"[\"a\xff\"]"),
            Err(StrictJsonError::NotUtf8 { offset: 3 })
        );
        assert_eq!(check_json_bytes("{\"k\":\"\u{e9}\"}".as_bytes()), Ok(()));
        // UTF-16 with a byte order mark is not the declared codec.
        assert!(check_json_bytes(&[0xff, 0xfe, b'1', 0]).is_err());
    }
}
