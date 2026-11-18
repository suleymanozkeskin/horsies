//! The refusals of Python's `loads_json` (`python/horsies/core/codec/json_io.py`),
//! with Python's texts, for a stored text whose refusal names them.
//!
//! `loads_json` runs `json.loads` of CPython 3.13 (its C scanner) with three
//! hooks, then checks the value. [`check_python_loads`] scans a text by the
//! same rules, in the same order, and gives the first refusal that Python
//! raises:
//!
//! - an empty text, before the scan (`EmptyJsonTextError`);
//! - a `JSONDecodeError` of the scanner: its message, then the line, the
//!   column and the index of the character (code points, not bytes);
//! - the `ValueError` of an int literal with more than [`MAX_INT_DIGITS`]
//!   digits;
//! - the hooks: a non-standard constant (`parse_constant`) and a float outside
//!   binary64 (`parse_float`) where the scan reads them, and an object that
//!   repeats a key (`object_pairs_hook`) where the object closes;
//! - after the scan: a lone surrogate (`_reject_lone_surrogate`), then a
//!   nesting deeper than [`MAX_JSON_DEPTH`] (`_reject_deep_nesting`).
//!
//! One difference: CPython's scanner recurses in C. A text nested deeper than
//! its C recursion limit (about 10,000 levels in CPython 3.13) raises
//! `RecursionError`, which `loads_json` reports with the text of a nesting
//! deeper than [`MAX_JSON_DEPTH`]. This scan does not recurse, so such a text
//! that also breaks the grammar after its nesting gets the grammar's text.
//! Both texts refuse the value.
//!
//! The scan builds no value, does no I/O and logs nothing. Its memory is
//! linear in the length of the text.

use std::collections::HashSet;

use super::{JsonConstant, MAX_INT_DIGITS, MAX_JSON_DEPTH};

/// The Python type of the value of a text that `loads_json` reads, as
/// `type(value).__name__` writes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PythonType {
    NoneType,
    Bool,
    Int,
    Float,
    Str,
    List,
    Dict,
}

impl PythonType {
    /// The name that `type(value).__name__` gives.
    pub fn python_name(self) -> &'static str {
        match self {
            Self::NoneType => "NoneType",
            Self::Bool => "bool",
            Self::Int => "int",
            Self::Float => "float",
            Self::Str => "str",
            Self::List => "list",
            Self::Dict => "dict",
        }
    }
}

/// Why `loads_json` refuses a text. The display is Python's text after
/// `JSON parse failed: `.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LoadsRefusal {
    /// The text is empty (`EmptyJsonTextError`). An empty text is not JSON.
    #[error("the text is empty")]
    EmptyText,
    /// A `JSONDecodeError` of the scanner.
    #[error("{message}: {position}")]
    Decode {
        message: DecodeMessage,
        position: TextPosition,
    },
    /// An int literal with more than [`MAX_INT_DIGITS`] digits; the sign
    /// does not count.
    #[error(
        "Exceeds the limit ({MAX_INT_DIGITS} digits) for integer string conversion: value has \
         {digits} digits; use sys.set_int_max_str_digits() to increase the limit"
    )]
    IntegerTooLong { digits: usize },
    /// `NaN`, `Infinity` or `-Infinity` (`_reject_nonstandard_json_constant`).
    #[error("Non-standard JSON constant is not allowed: {0}")]
    NonStandardConstant(JsonConstant),
    /// A float literal outside finite binary64 (`_parse_finite_float`).
    #[error("JSON number is outside the finite binary64 range")]
    NonFiniteFloat,
    /// An object that repeats a key, after unescaping
    /// (`_object_without_repeated_key`).
    #[error("JSON object repeats a key")]
    RepeatedKey,
    /// A string or a key that holds a lone UTF-16 surrogate
    /// (`_reject_lone_surrogate`).
    #[error("JSON string holds a lone surrogate")]
    LoneSurrogate,
    /// Arrays and objects nested deeper than [`MAX_JSON_DEPTH`]
    /// (`JSON_TEXT_TOO_DEEP`).
    #[error("JSON text is nested deeper than {MAX_JSON_DEPTH} levels")]
    TooDeep,
}

/// The message of a `JSONDecodeError` of CPython's scanner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeMessage {
    UnexpectedBom,
    ExpectingValue,
    ExpectingPropertyName,
    ExpectingColon,
    ExpectingComma,
    TrailingCommaInArray,
    TrailingCommaInObject,
    UnterminatedString,
    InvalidControlCharacter,
    InvalidEscape,
    InvalidUnicodeEscape,
    ExtraData,
}

impl std::fmt::Display for DecodeMessage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::UnexpectedBom => "Unexpected UTF-8 BOM (decode using utf-8-sig)",
            Self::ExpectingValue => "Expecting value",
            Self::ExpectingPropertyName => "Expecting property name enclosed in double quotes",
            Self::ExpectingColon => "Expecting ':' delimiter",
            Self::ExpectingComma => "Expecting ',' delimiter",
            Self::TrailingCommaInArray => "Illegal trailing comma before end of array",
            Self::TrailingCommaInObject => "Illegal trailing comma before end of object",
            Self::UnterminatedString => "Unterminated string starting at",
            Self::InvalidControlCharacter => "Invalid control character at",
            Self::InvalidEscape => "Invalid \\escape",
            Self::InvalidUnicodeEscape => "Invalid \\uXXXX escape",
            Self::ExtraData => "Extra data",
        })
    }
}

/// Where a `JSONDecodeError` is: the line and the column from 1, and the
/// index of the character from 0, all counted in code points.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TextPosition {
    pub line: usize,
    pub column: usize,
    pub char_index: usize,
}

impl std::fmt::Display for TextPosition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "line {} column {} (char {})",
            self.line, self.column, self.char_index
        )
    }
}

/// Scan `text` by the rules of Python's `loads_json`. `Ok` holds the type of
/// the value that Python reads; `Err` the first refusal that Python raises.
/// An empty text is [`LoadsRefusal::EmptyText`]: `loads_json` refuses it
/// before the scan, with that text. A caller with a nullable column checks
/// NULL before the call. Pure.
pub fn check_python_loads(text: &str) -> Result<PythonType, LoadsRefusal> {
    if text.is_empty() {
        return Err(LoadsRefusal::EmptyText);
    }
    let chars: Vec<char> = text.chars().collect();
    Scanner {
        chars: &chars,
        lone_surrogate: false,
        max_depth: 0,
    }
    .run()
}

/// What the scan reads next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Read {
    /// A value.
    Value,
    /// An object key and its `:`.
    Key,
    /// The end of a value: the close of its container, or a `,`.
    AfterValue,
}

/// A container that the scan is inside.
enum Frame {
    Array,
    /// The keys read so far, as code points, a joined surrogate pair as one.
    Object {
        keys: HashSet<Vec<u32>>,
        repeats_a_key: bool,
    },
}

/// The kind of container that a bracket opens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Container {
    Array,
    Object,
}

struct Scanner<'a> {
    chars: &'a [char],
    lone_surrogate: bool,
    max_depth: usize,
}

impl Scanner<'_> {
    /// The scan of the whole text (`json.loads`, then `_reject_lone_surrogate`
    /// and `_reject_deep_nesting`). Each turn of the loop reads at least one
    /// character or ends the scan, so the loop ends within the text.
    fn run(mut self) -> Result<PythonType, LoadsRefusal> {
        if self.chars.first() == Some(&'\u{feff}') {
            return Err(self.decode_error(DecodeMessage::UnexpectedBom, 0));
        }
        let mut stack: Vec<Frame> = Vec::new();
        let mut top: Option<PythonType> = None;
        let mut index = self.skip_whitespace(0);
        let mut read = Read::Value;
        loop {
            match read {
                Read::Value => {
                    let (kind, next, then) = self.value(index, &mut stack)?;
                    top = top.or(Some(kind));
                    (index, read) = (next, then);
                }
                Read::Key => {
                    index = self.key(index, &mut stack)?;
                    read = Read::Value;
                }
                Read::AfterValue if stack.is_empty() => break,
                Read::AfterValue => (index, read) = self.after_value(index, &mut stack)?,
            }
        }
        let end = self.skip_whitespace(index);
        if end != self.chars.len() {
            return Err(self.decode_error(DecodeMessage::ExtraData, end));
        }
        match (self.lone_surrogate, self.max_depth > MAX_JSON_DEPTH) {
            (true, _) => Err(LoadsRefusal::LoneSurrogate),
            (false, true) => Err(LoadsRefusal::TooDeep),
            (false, false) => Ok(top.expect("a scan that ends has read the top value")),
        }
    }

    /// Read the value at `index` (CPython `scan_once_unicode`): a scalar, an
    /// empty container, or the opening of a container, which goes on
    /// `stack`. Returns its type, the index after what was read, and what
    /// comes next.
    fn value(
        &mut self,
        index: usize,
        stack: &mut Vec<Frame>,
    ) -> Result<(PythonType, usize, Read), LoadsRefusal> {
        let Some(first) = self.chars.get(index).copied() else {
            return Err(self.decode_error(DecodeMessage::ExpectingValue, index));
        };
        match first {
            '"' => {
                let (_, end) = self.string(index)?;
                Ok((PythonType::Str, end, Read::AfterValue))
            }
            '[' => Ok(self.open(Container::Array, index, stack)),
            '{' => Ok(self.open(Container::Object, index, stack)),
            _ if self.starts_with(index, "null") => {
                Ok((PythonType::NoneType, index + 4, Read::AfterValue))
            }
            _ if self.starts_with(index, "true") => {
                Ok((PythonType::Bool, index + 4, Read::AfterValue))
            }
            _ if self.starts_with(index, "false") => {
                Ok((PythonType::Bool, index + 5, Read::AfterValue))
            }
            _ if self.starts_with(index, "NaN") => {
                Err(LoadsRefusal::NonStandardConstant(JsonConstant::Nan))
            }
            _ if self.starts_with(index, "Infinity") => {
                Err(LoadsRefusal::NonStandardConstant(JsonConstant::Infinity))
            }
            _ if self.starts_with(index, "-Infinity") => Err(LoadsRefusal::NonStandardConstant(
                JsonConstant::NegativeInfinity,
            )),
            _ => {
                let (kind, end) = self.number(index)?;
                Ok((kind, end, Read::AfterValue))
            }
        }
    }

    /// Open the container whose bracket is at `index`. An empty container is
    /// a whole value; another one goes on `stack`, and its first item or key
    /// comes next. Every container counts for the depth, an empty one too.
    fn open(
        &mut self,
        container: Container,
        index: usize,
        stack: &mut Vec<Frame>,
    ) -> (PythonType, usize, Read) {
        self.max_depth = self.max_depth.max(stack.len() + 1);
        let inside = self.skip_whitespace(index + 1);
        let (kind, close) = match container {
            Container::Array => (PythonType::List, ']'),
            Container::Object => (PythonType::Dict, '}'),
        };
        if self.chars.get(inside).copied() == Some(close) {
            return (kind, inside + 1, Read::AfterValue);
        }
        match container {
            Container::Array => {
                stack.push(Frame::Array);
                (kind, inside, Read::Value)
            }
            Container::Object => {
                stack.push(Frame::Object {
                    keys: HashSet::new(),
                    repeats_a_key: false,
                });
                (kind, inside, Read::Key)
            }
        }
    }

    /// Read the key at `index` of the innermost object, then its `:`
    /// (CPython `_parse_object_unicode`). Returns the index of its value.
    fn key(&mut self, index: usize, stack: &mut [Frame]) -> Result<usize, LoadsRefusal> {
        if self.chars.get(index).copied() != Some('"') {
            return Err(self.decode_error(DecodeMessage::ExpectingPropertyName, index));
        }
        let (key, end) = self.string(index)?;
        match stack.last_mut() {
            Some(Frame::Object {
                keys,
                repeats_a_key,
            }) => *repeats_a_key |= !keys.insert(key),
            Some(Frame::Array) | None => unreachable!("a key is read inside an object"),
        }
        let colon = self.skip_whitespace(end);
        if self.chars.get(colon).copied() != Some(':') {
            return Err(self.decode_error(DecodeMessage::ExpectingColon, colon));
        }
        Ok(self.skip_whitespace(colon + 1))
    }

    /// Read what follows a value inside the innermost container: its close,
    /// or a `,` and the next item or key (CPython `_parse_array_unicode`,
    /// `_parse_object_unicode`). An object that repeats a key is refused at
    /// its close, where Python's `object_pairs_hook` runs.
    fn after_value(
        &self,
        index: usize,
        stack: &mut Vec<Frame>,
    ) -> Result<(usize, Read), LoadsRefusal> {
        let (close, trailing_comma, next_item) = match stack.last() {
            Some(Frame::Array) => (']', DecodeMessage::TrailingCommaInArray, Read::Value),
            Some(Frame::Object { .. }) => ('}', DecodeMessage::TrailingCommaInObject, Read::Key),
            None => unreachable!("a value ends inside a container here"),
        };
        let index = self.skip_whitespace(index);
        match self.chars.get(index).copied() {
            Some(found) if found == close => match stack.pop() {
                Some(Frame::Object {
                    repeats_a_key: true,
                    ..
                }) => Err(LoadsRefusal::RepeatedKey),
                Some(
                    Frame::Object {
                        repeats_a_key: false,
                        ..
                    }
                    | Frame::Array,
                ) => Ok((index + 1, Read::AfterValue)),
                None => unreachable!("the closed container was on the stack"),
            },
            Some(',') => {
                let next = self.skip_whitespace(index + 1);
                match self.chars.get(next).copied() {
                    Some(found) if found == close => Err(self.decode_error(trailing_comma, index)),
                    Some(_) | None => Ok((next, next_item)),
                }
            }
            Some(_) | None => Err(self.decode_error(DecodeMessage::ExpectingComma, index)),
        }
    }

    /// Read the string whose opening quote is at `quote` (CPython
    /// `scanstring_unicode`, strict). Returns its code points, a joined
    /// surrogate pair as one, and the index after its closing quote.
    fn string(&mut self, quote: usize) -> Result<(Vec<u32>, usize), LoadsRefusal> {
        let mut decoded: Vec<u32> = Vec::new();
        let mut end = quote + 1;
        loop {
            let mut next = end;
            while let Some(character) = self.chars.get(next).copied() {
                match character {
                    '"' | '\\' => break,
                    _ if u32::from(character) <= 0x1f => {
                        return Err(self.decode_error(DecodeMessage::InvalidControlCharacter, next))
                    }
                    _ => {
                        decoded.push(u32::from(character));
                        next += 1;
                    }
                }
            }
            match self.chars.get(next).copied() {
                Some('"') => return Ok((decoded, next + 1)),
                Some(_) => {}
                None => return Err(self.decode_error(DecodeMessage::UnterminatedString, quote)),
            }
            // A backslash is at `next`.
            end = match self.chars.get(next + 1).copied() {
                None => return Err(self.decode_error(DecodeMessage::UnterminatedString, quote)),
                Some('u') => self.unicode_escape(next + 1, &mut decoded)?,
                Some(escaped) => match simple_escape(escaped) {
                    Some(unescaped) => {
                        decoded.push(u32::from(unescaped));
                        next + 2
                    }
                    None => return Err(self.decode_error(DecodeMessage::InvalidEscape, next)),
                },
            };
        }
    }

    /// Read the `\u` escape whose `u` is at `u_index`, and a second escape
    /// that completes a surrogate pair, as CPython reads them: the four
    /// digits need one more character after them, and a second escape needs
    /// one more character after its own digits. Pushes the code point, or the
    /// lone surrogate, and returns the index after what was read.
    fn unicode_escape(
        &mut self,
        u_index: usize,
        decoded: &mut Vec<u32>,
    ) -> Result<usize, LoadsRefusal> {
        let end = u_index + 5;
        if end >= self.chars.len() {
            return Err(self.decode_error(DecodeMessage::InvalidUnicodeEscape, u_index));
        }
        let Some(unit) = self.hex4(u_index + 1) else {
            return Err(self.decode_error(DecodeMessage::InvalidUnicodeEscape, u_index));
        };
        let second_escape =
            end + 6 < self.chars.len() && self.chars[end] == '\\' && self.chars[end + 1] == 'u';
        if (0xD800..=0xDBFF).contains(&unit) && second_escape {
            let Some(low) = self.hex4(end + 2) else {
                return Err(self.decode_error(DecodeMessage::InvalidUnicodeEscape, end + 1));
            };
            if (0xDC00..=0xDFFF).contains(&low) {
                decoded.push(0x10000 + ((unit - 0xD800) << 10) + (low - 0xDC00));
                return Ok(end + 6);
            }
        }
        if (0xD800..=0xDFFF).contains(&unit) {
            self.lone_surrogate = true;
        }
        decoded.push(unit);
        Ok(end)
    }

    /// Read the number at `start` (CPython `_match_number_unicode`), then
    /// convert it as Python does: an int of more than [`MAX_INT_DIGITS`]
    /// digits and a float outside binary64 are refused.
    fn number(&self, start: usize) -> Result<(PythonType, usize), LoadsRefusal> {
        let chars = self.chars;
        let is_digit = |index: usize| chars.get(index).is_some_and(char::is_ascii_digit);
        let mut index = start;
        if chars.get(index).copied() == Some('-') {
            index += 1;
        }
        match chars.get(index).copied() {
            Some('1'..='9') => {
                index += 1;
                while is_digit(index) {
                    index += 1;
                }
            }
            Some('0') => index += 1,
            Some(_) | None => return Err(self.decode_error(DecodeMessage::ExpectingValue, start)),
        }
        let mut is_float = false;
        if index + 1 < chars.len() && chars[index] == '.' && is_digit(index + 1) {
            is_float = true;
            index += 2;
            while is_digit(index) {
                index += 1;
            }
        }
        if index + 1 < chars.len() && matches!(chars[index], 'e' | 'E') {
            let exponent_start = index;
            index += 1;
            if index + 1 < chars.len() && matches!(chars[index], '-' | '+') {
                index += 1;
            }
            while is_digit(index) {
                index += 1;
            }
            match is_digit(index - 1) {
                true => is_float = true,
                false => index = exponent_start,
            }
        }
        let literal = &chars[start..index];
        match is_float {
            true => {
                let text: String = literal.iter().collect();
                let value: f64 = text.parse().expect("a JSON float literal parses as an f64");
                match value.is_finite() {
                    true => Ok((PythonType::Float, index)),
                    false => Err(LoadsRefusal::NonFiniteFloat),
                }
            }
            false => match literal
                .iter()
                .filter(|character| character.is_ascii_digit())
                .count()
            {
                digits if digits > MAX_INT_DIGITS => Err(LoadsRefusal::IntegerTooLong { digits }),
                _ => Ok((PythonType::Int, index)),
            },
        }
    }

    /// The four hex digits from `start` as a code unit, or `None` when one
    /// is not a hex digit.
    fn hex4(&self, start: usize) -> Option<u32> {
        self.chars
            .get(start..start + 4)?
            .iter()
            .try_fold(0_u32, |unit, character| {
                character.to_digit(16).map(|digit| unit * 16 + digit)
            })
    }

    fn starts_with(&self, index: usize, literal: &str) -> bool {
        literal
            .chars()
            .enumerate()
            .all(|(offset, expected)| self.chars.get(index + offset).copied() == Some(expected))
    }

    /// The index after the JSON whitespace (space, tab, line feed, carriage
    /// return) from `index`.
    fn skip_whitespace(&self, mut index: usize) -> usize {
        while matches!(
            self.chars.get(index).copied(),
            Some(' ' | '\t' | '\n' | '\r')
        ) {
            index += 1;
        }
        index
    }

    /// The `JSONDecodeError` of `message` at `char_index`: the line is 1 plus
    /// the line feeds before it, and the column counts from the last line
    /// feed before it (`json.decoder.JSONDecodeError`).
    fn decode_error(&self, message: DecodeMessage, char_index: usize) -> LoadsRefusal {
        let before = &self.chars[..char_index.min(self.chars.len())];
        let line = 1 + before
            .iter()
            .filter(|&&character| character == '\n')
            .count();
        let column = match before.iter().rposition(|&character| character == '\n') {
            Some(line_feed) => char_index - line_feed,
            None => char_index + 1,
        };
        LoadsRefusal::Decode {
            message,
            position: TextPosition {
                line,
                column,
                char_index,
            },
        }
    }
}

/// The character of a one-letter escape, or `None` for a letter that JSON
/// does not escape.
fn simple_escape(escaped: char) -> Option<char> {
    match escaped {
        '"' => Some('"'),
        '\\' => Some('\\'),
        '/' => Some('/'),
        'b' => Some('\u{8}'),
        'f' => Some('\u{c}'),
        'n' => Some('\n'),
        'r' => Some('\r'),
        't' => Some('\t'),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
