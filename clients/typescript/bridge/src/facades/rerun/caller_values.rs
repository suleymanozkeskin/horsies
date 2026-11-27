//! A caller statement and its row as they cross to and from the package,
//! each value in its PostgreSQL text form.
//!
//! - A parameter crosses as its PostgreSQL text input, or SQL NULL. The
//!   statement's SQL text casts each parameter (`$1::uuid`), so the server
//!   reads the text as the declared type and the driver infers nothing:
//!   `int8` as decimal text, `timestamptz` as RFC 3339 text with
//!   microseconds, `bytea` as `\x` and hexadecimal digits, `uuid` as
//!   lower-case hyphenated text.
//! - A row crosses as one cell per result column, in column order: the
//!   text that the server sends in text format, or SQL NULL. This module
//!   reads each text as the column's declared type; a `bytea` text in its
//!   hex output or in its escape output, so the session's `bytea_output`
//!   does not change the row. A cell count or a text that does not fit is a
//!   [`RowTextRefused`]: the caller answers `CONTRACT_VIOLATION`.

use std::fmt;

use chrono::{DateTime, SecondsFormat, Utc};
use horsies_core::{CallerColumn, CallerRow, CallerStatement, CallerType, StatementValue};
use napi_derive::napi;

use crate::values::boundary::{bytes_of_hex, text_of_uuid, uuid_of_text};

/// The text form of one SQL value.
#[napi(discriminant = "kind", discriminant_case = "camelCase")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SqlTextValue {
    /// The PostgreSQL text input or output of the value.
    Text { text: String },
    /// SQL NULL.
    Null,
}

/// A built-in PostgreSQL type of a caller statement, by its SQL name.
#[napi(string_enum)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallerTypeName {
    #[napi(value = "bool")]
    Bool,
    #[napi(value = "int2")]
    Int2,
    #[napi(value = "int4")]
    Int4,
    #[napi(value = "int8")]
    Int8,
    #[napi(value = "text")]
    Text,
    #[napi(value = "bytea")]
    Bytea,
    #[napi(value = "uuid")]
    Uuid,
    #[napi(value = "timestamptz")]
    Timestamptz,
}

/// One result column of a caller statement.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CallerColumnRecord {
    pub name: String,
    pub value_type: CallerTypeName,
}

/// One core statement for a transaction that the caller owns.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CallerStatementRecord {
    /// The statement's name, for errors and logs.
    pub name: String,
    /// Positional SQL text (`$1`, `$2`, ...) with a cast on each parameter.
    pub sql: String,
    /// One value per parameter, `$1` first.
    pub parameters: Vec<SqlTextValue>,
    /// The result columns, in the order of the row.
    pub columns: Vec<CallerColumnRecord>,
}

/// The one row that the caller's driver read for a caller statement: one
/// cell per result column, in column order.
#[napi(object, object_to_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CallerRowArg {
    pub cells: Vec<SqlTextValue>,
}

/// The rule of a `bool` cell.
const BOOL_RULE: &str = "t or f, the text output of bool";

/// The rule of an integer cell.
const INTEGER_RULE: &str = "a decimal integer in the range of the column type";

/// The prefix of the hex text of `bytea`.
const BYTEA_HEX_PREFIX: &str = "\\x";

/// The escape byte of the escape text of `bytea`: a backslash.
const BYTEA_ESCAPE: u8 = b'\\';

/// The rule of a `bytea` cell in its hex form.
const BYTEA_HEX_RULE: &str = "\\x and lower-case hexadecimal digits, the hex output of bytea \
                              (bytea_output = hex)";

/// The rule of a `bytea` cell in its escape form.
const BYTEA_ESCAPE_RULE: &str = "the escape output of bytea (bytea_output = escape): \\\\ for a \
                                 backslash, a backslash and three octal digits from 000 to 377 \
                                 for one byte, any other byte as itself";

/// The rule of a `uuid` cell.
const UUID_RULE: &str = "lower-case hyphenated UUID text, the text output of uuid";

/// The rule of a `timestamptz` cell.
const TIMESTAMPTZ_RULE: &str = "YYYY-MM-DD HH:MM:SS[.ffffff]+HH[:MM], the text output of \
                                timestamptz under DateStyle ISO";

/// The parse format of the ISO text output of `timestamptz`.
const TIMESTAMPTZ_ISO_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f%#z";

/// The hexadecimal digits, by value.
const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

/// Why a row in text form does not fit the columns of its statement: a
/// defect of the adapter that read it, or a row outside the function's
/// contract. The same row meets it again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RowTextRefused {
    /// The row has another count of cells than the statement has columns.
    CountMismatch {
        statement: &'static str,
        declared: usize,
        given: usize,
    },
    /// The text of a cell breaks the rule of its column's type.
    Cell {
        statement: &'static str,
        column: &'static str,
        declared: &'static str,
        rule: &'static str,
    },
}

impl fmt::Display for RowTextRefused {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CountMismatch {
                statement,
                declared,
                given,
            } => write!(
                formatter,
                "statement `{statement}` declares {declared} result columns; the row has {given} cells"
            ),
            Self::Cell {
                statement,
                column,
                declared,
                rule,
            } => write!(
                formatter,
                "statement `{statement}` column `{column}` is {declared}; its text is not {rule}"
            ),
        }
    }
}

/// The record of `statement`: its name, its positional SQL text, each
/// parameter as text, and its columns. Pure.
pub fn statement_record(statement: &CallerStatement) -> CallerStatementRecord {
    CallerStatementRecord {
        name: statement.name().to_owned(),
        sql: statement.sql().to_owned(),
        parameters: statement.parameters().iter().map(parameter_text).collect(),
        columns: statement
            .columns()
            .iter()
            .map(|column| CallerColumnRecord {
                name: column.name.to_owned(),
                value_type: type_name(column.value_type),
            })
            .collect(),
    }
}

/// The name of `value_type`. Pure.
fn type_name(value_type: CallerType) -> CallerTypeName {
    match value_type {
        CallerType::Bool => CallerTypeName::Bool,
        CallerType::Int2 => CallerTypeName::Int2,
        CallerType::Int4 => CallerTypeName::Int4,
        CallerType::Int8 => CallerTypeName::Int8,
        CallerType::Text => CallerTypeName::Text,
        CallerType::Bytea => CallerTypeName::Bytea,
        CallerType::Uuid => CallerTypeName::Uuid,
        CallerType::Timestamptz => CallerTypeName::Timestamptz,
    }
}

/// The PostgreSQL text input of `value`, or SQL NULL. Pure.
pub fn parameter_text(value: &StatementValue) -> SqlTextValue {
    let text = match value {
        StatementValue::Bool(true) => "true".to_owned(),
        StatementValue::Bool(false) => "false".to_owned(),
        StatementValue::Int2(number) => number.to_string(),
        StatementValue::Int4(number) => number.to_string(),
        StatementValue::Int8(number) => number.to_string(),
        StatementValue::Text(text) => text.clone(),
        StatementValue::Bytea(bytes) => bytea_text(bytes),
        StatementValue::Uuid(id) => text_of_uuid(*id),
        StatementValue::Timestamptz(instant) => {
            instant.to_rfc3339_opts(SecondsFormat::Micros, true)
        }
        StatementValue::Null(_) => return SqlTextValue::Null,
    };
    SqlTextValue::Text { text }
}

/// The hex text input of `bytes`: `\x` and two lower-case digits a byte.
/// Pure.
fn bytea_text(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(BYTEA_HEX_PREFIX.len() + bytes.len() * 2);
    text.push_str(BYTEA_HEX_PREFIX);
    for byte in bytes.iter().copied() {
        text.push(char::from(HEX_DIGITS[usize::from(byte >> 4)]));
        text.push(char::from(HEX_DIGITS[usize::from(byte & 0x0f)]));
    }
    text
}

/// The row of `statement` from its cells in text form. Pure.
///
/// Errors: [`RowTextRefused`] for another count of cells, or a cell text
/// that breaks the rule of its column's type.
pub fn caller_row(
    statement: &CallerStatement,
    row: CallerRowArg,
) -> Result<CallerRow, RowTextRefused> {
    let columns = statement.columns();
    if row.cells.len() != columns.len() {
        return Err(RowTextRefused::CountMismatch {
            statement: statement.name(),
            declared: columns.len(),
            given: row.cells.len(),
        });
    }
    columns
        .iter()
        .zip(row.cells)
        .map(|(column, cell)| {
            cell_value(column, cell).map_err(|rule| RowTextRefused::Cell {
                statement: statement.name(),
                column: column.name,
                declared: column.value_type.sql_name(),
                rule,
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map(CallerRow::new)
}

/// The value of one cell of `column`, or the rule that its text breaks.
/// Pure.
fn cell_value(column: &CallerColumn, cell: SqlTextValue) -> Result<StatementValue, &'static str> {
    let text = match cell {
        SqlTextValue::Null => return Ok(StatementValue::Null(column.value_type)),
        SqlTextValue::Text { text } => text,
    };
    match column.value_type {
        CallerType::Bool => bool_of_text(&text).map(StatementValue::Bool),
        CallerType::Int2 => text
            .parse()
            .map(StatementValue::Int2)
            .map_err(|_| INTEGER_RULE),
        CallerType::Int4 => text
            .parse()
            .map(StatementValue::Int4)
            .map_err(|_| INTEGER_RULE),
        CallerType::Int8 => text
            .parse()
            .map(StatementValue::Int8)
            .map_err(|_| INTEGER_RULE),
        CallerType::Text => Ok(StatementValue::Text(text)),
        CallerType::Bytea => bytea_of_text(&text).map(StatementValue::Bytea),
        CallerType::Uuid => uuid_of_text(column.name, &text)
            .map(StatementValue::Uuid)
            .map_err(|_| UUID_RULE),
        CallerType::Timestamptz => instant_of_iso_text(&text).map(StatementValue::Timestamptz),
    }
}

/// The flag of the text output of `bool`. Pure.
fn bool_of_text(text: &str) -> Result<bool, &'static str> {
    match text {
        "t" => Ok(true),
        "f" => Ok(false),
        _other => Err(BOOL_RULE),
    }
}

/// The bytes of the text output of `bytea`: the hex form (`\x` and two
/// digits a byte) or the escape form. The escape form never starts with
/// `\x`: its backslash is doubled or starts three octal digits. Pure.
fn bytea_of_text(text: &str) -> Result<Vec<u8>, &'static str> {
    match text.strip_prefix(BYTEA_HEX_PREFIX) {
        Some(digits) => bytes_of_hex("bytea", digits).map_err(|_| BYTEA_HEX_RULE),
        None => bytes_of_escape_text(text.as_bytes()),
    }
}

/// The bytes of the escape text output of `bytea`, by the escape input rule
/// of PostgreSQL: `\\` is one backslash, a backslash and three octal digits
/// (`000` to `377`) is one byte, and any other byte is itself. Each step
/// reads one byte or one sequence, so the loop ends at the end of `text`.
/// Pure.
///
/// Errors: [`BYTEA_ESCAPE_RULE`] for a backslash that starts neither
/// sequence.
fn bytes_of_escape_text(text: &[u8]) -> Result<Vec<u8>, &'static str> {
    let mut bytes = Vec::with_capacity(text.len());
    let mut rest = text;
    while let Some((&first, after_first)) = rest.split_first() {
        let (byte, after_byte) = match first {
            BYTEA_ESCAPE => escaped_byte(after_first)?,
            literal => (literal, after_first),
        };
        bytes.push(byte);
        rest = after_byte;
    }
    Ok(bytes)
}

/// The byte of one escape sequence of `bytea`, from the text after its
/// backslash, and the text after the sequence. Pure.
fn escaped_byte(after_escape: &[u8]) -> Result<(u8, &[u8]), &'static str> {
    match after_escape {
        [BYTEA_ESCAPE, rest @ ..] => Ok((BYTEA_ESCAPE, rest)),
        [high @ b'0'..=b'3', middle @ b'0'..=b'7', low @ b'0'..=b'7', rest @ ..] => {
            Ok((octal_byte(*high, *middle, *low), rest))
        }
        _other => Err(BYTEA_ESCAPE_RULE),
    }
}

/// The byte of three octal digits, the first from `0` to `3`: at most
/// `0o377`, 255. Pure.
fn octal_byte(high: u8, middle: u8, low: u8) -> u8 {
    (high - b'0') * 64 + (middle - b'0') * 8 + (low - b'0')
}

/// The instant of the ISO text output of `timestamptz`. Pure.
fn instant_of_iso_text(text: &str) -> Result<DateTime<Utc>, &'static str> {
    DateTime::parse_from_str(text, TIMESTAMPTZ_ISO_FORMAT)
        .map(|instant| instant.with_timezone(&Utc))
        .map_err(|_| TIMESTAMPTZ_RULE)
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone};
    use horsies_core::{rerun_read, EnqueueRetention, RerunCommand, RerunEnqueuePolicy};
    use uuid::Uuid;

    use super::*;

    const SOURCE: Uuid = Uuid::from_u128(0x0189_0a5d_ac96_774b_bcce_b302_099a_8057);

    fn text(value: &str) -> SqlTextValue {
        SqlTextValue::Text {
            text: value.to_owned(),
        }
    }

    fn column(value_type: CallerType) -> CallerColumn {
        CallerColumn {
            name: "cell",
            value_type,
        }
    }

    fn read_statement() -> CallerStatement {
        let policy = RerunEnqueuePolicy::new(EnqueueRetention::Forever, true, Duration::hours(24))
            .expect("a valid policy");
        rerun_read(&RerunCommand::new(SOURCE, None, None), &policy)
    }

    #[test]
    fn each_parameter_type_has_its_text_input() {
        let instant = Utc
            .with_ymd_and_hms(2026, 11, 26, 9, 10, 12)
            .single()
            .expect("a valid instant")
            + Duration::microseconds(654_321);
        let cases = [
            (StatementValue::Bool(true), text("true")),
            (StatementValue::Bool(false), text("false")),
            (StatementValue::Int2(-7), text("-7")),
            (StatementValue::Int4(2_147_483_647), text("2147483647")),
            (StatementValue::Int8(i64::MIN), text("-9223372036854775808")),
            (
                StatementValue::Text("a 'quoted' text".to_owned()),
                text("a 'quoted' text"),
            ),
            (
                StatementValue::Bytea(vec![0x00, 0x0a, 0xff]),
                text("\\x000aff"),
            ),
            (StatementValue::Bytea(Vec::new()), text("\\x")),
            (
                StatementValue::Uuid(SOURCE),
                text("01890a5d-ac96-774b-bcce-b302099a8057"),
            ),
            (
                StatementValue::Timestamptz(instant),
                text("2026-11-26T09:10:12.654321Z"),
            ),
            (
                StatementValue::Null(CallerType::Timestamptz),
                SqlTextValue::Null,
            ),
        ];
        for (value, expected) in cases {
            assert_eq!(parameter_text(&value), expected, "{value:?}");
        }
    }

    #[test]
    fn a_whole_second_keeps_six_fraction_digits() {
        let instant = Utc
            .with_ymd_and_hms(1, 1, 1, 0, 0, 0)
            .single()
            .expect("a valid instant");
        assert_eq!(
            parameter_text(&StatementValue::Timestamptz(instant)),
            text("0001-01-01T00:00:00.000000Z")
        );
    }

    #[test]
    fn the_record_of_the_source_read_casts_and_lists_its_columns() {
        let record = statement_record(&read_statement());
        assert_eq!(record.name, "rerun_source_read");
        assert!(
            record
                .sql
                .ends_with("FROM horsies_rerun_source_read($1::uuid, $2::text)"),
            "{}",
            record.sql
        );
        assert_eq!(
            record.parameters,
            vec![
                text("01890a5d-ac96-774b-bcce-b302099a8057"),
                text("forever")
            ]
        );
        assert_eq!(record.columns.len(), 19);
        assert_eq!(
            record.columns[0],
            CallerColumnRecord {
                name: "class_registered".to_owned(),
                value_type: CallerTypeName::Bool,
            }
        );
        assert_eq!(record.columns[18].value_type, CallerTypeName::Bytea);
    }

    #[test]
    fn each_cell_type_reads_its_text_output() {
        let instant = Utc
            .with_ymd_and_hms(2026, 11, 26, 9, 10, 12)
            .single()
            .expect("a valid instant");
        let cases = [
            (CallerType::Bool, "t", StatementValue::Bool(true)),
            (CallerType::Bool, "f", StatementValue::Bool(false)),
            (CallerType::Int2, "-32768", StatementValue::Int2(i16::MIN)),
            (CallerType::Int4, "7", StatementValue::Int4(7)),
            (
                CallerType::Int8,
                "9223372036854775807",
                StatementValue::Int8(i64::MAX),
            ),
            (CallerType::Text, "", StatementValue::Text(String::new())),
            (
                CallerType::Bytea,
                "\\x0aff",
                StatementValue::Bytea(vec![0x0a, 0xff]),
            ),
            (
                CallerType::Uuid,
                "01890a5d-ac96-774b-bcce-b302099a8057",
                StatementValue::Uuid(SOURCE),
            ),
            (
                CallerType::Timestamptz,
                "2026-11-26 09:10:12.654321+00",
                StatementValue::Timestamptz(instant + Duration::microseconds(654_321)),
            ),
            (
                CallerType::Timestamptz,
                "2026-11-26 14:40:12+05:30",
                StatementValue::Timestamptz(instant),
            ),
        ];
        for (value_type, cell, expected) in cases {
            assert_eq!(
                cell_value(&column(value_type), text(cell)),
                Ok(expected),
                "{value_type:?} {cell}"
            );
        }
    }

    #[test]
    fn a_bytea_cell_in_hex_form_reads_its_bytes() {
        let cases: [(&str, &[u8]); 3] = [
            ("\\x", &[]),
            ("\\x000aff", &[0x00, 0x0a, 0xff]),
            ("\\x5c78", b"\\x"),
        ];
        for (cell, bytes) in cases {
            assert_eq!(
                cell_value(&column(CallerType::Bytea), text(cell)),
                Ok(StatementValue::Bytea(bytes.to_vec())),
                "{cell}"
            );
        }
    }

    #[test]
    fn a_bytea_cell_in_escape_form_reads_its_bytes() {
        // The escape output of PostgreSQL for each byte string: a printable
        // byte as itself, a backslash doubled, any other byte as three octal
        // digits.
        let cases: [(&str, &[u8]); 6] = [
            ("", &[]),
            ("0aff", b"0aff"),
            ("\\\\", b"\\"),
            ("\\\\x", b"\\x"),
            ("\\000\\012\\377", &[0x00, 0x0a, 0xff]),
            ("a \\001~\\\\", &[b'a', b' ', 0x01, b'~', b'\\']),
        ];
        for (cell, bytes) in cases {
            assert_eq!(
                cell_value(&column(CallerType::Bytea), text(cell)),
                Ok(StatementValue::Bytea(bytes.to_vec())),
                "{cell}"
            );
        }
    }

    #[test]
    fn a_bytea_cell_outside_its_form_names_the_rule_of_the_form() {
        let cases = [
            ("\\x0AFF", BYTEA_HEX_RULE),
            ("\\x0af", BYTEA_HEX_RULE),
            ("\\x\\000", BYTEA_HEX_RULE),
            ("ab\\", BYTEA_ESCAPE_RULE),
            ("\\400", BYTEA_ESCAPE_RULE),
            ("\\01", BYTEA_ESCAPE_RULE),
            ("\\08x", BYTEA_ESCAPE_RULE),
            ("\\X0aff", BYTEA_ESCAPE_RULE),
        ];
        for (cell, rule) in cases {
            assert_eq!(
                cell_value(&column(CallerType::Bytea), text(cell)),
                Err(rule),
                "{cell}"
            );
        }
    }

    #[test]
    fn a_null_cell_is_null_of_its_column_type() {
        assert_eq!(
            cell_value(&column(CallerType::Int4), SqlTextValue::Null),
            Ok(StatementValue::Null(CallerType::Int4))
        );
    }

    #[test]
    fn a_cell_text_outside_its_type_names_the_rule() {
        let cases = [
            (CallerType::Bool, "true", BOOL_RULE),
            (CallerType::Int2, "32768", INTEGER_RULE),
            (CallerType::Int4, "7.0", INTEGER_RULE),
            (CallerType::Int8, "", INTEGER_RULE),
            (
                CallerType::Uuid,
                "01890A5D-AC96-774B-BCCE-B302099A8057",
                UUID_RULE,
            ),
            (CallerType::Timestamptz, "infinity", TIMESTAMPTZ_RULE),
            (
                CallerType::Timestamptz,
                "Thu Nov 26 09:10:12 2026 UTC",
                TIMESTAMPTZ_RULE,
            ),
        ];
        for (value_type, cell, rule) in cases {
            assert_eq!(
                cell_value(&column(value_type), text(cell)),
                Err(rule),
                "{value_type:?} {cell}"
            );
        }
    }

    #[test]
    fn a_row_of_another_cell_count_is_refused() {
        let refused = caller_row(
            &read_statement(),
            CallerRowArg {
                cells: vec![text("t")],
            },
        );
        assert_eq!(
            refused,
            Err(RowTextRefused::CountMismatch {
                statement: "rerun_source_read",
                declared: 19,
                given: 1,
            })
        );
    }

    #[test]
    fn a_cell_that_breaks_its_rule_names_the_statement_and_the_column() {
        let mut cells = vec![SqlTextValue::Null; 19];
        cells[0] = text("yes");
        let refused = caller_row(&read_statement(), CallerRowArg { cells });
        assert_eq!(
            refused,
            Err(RowTextRefused::Cell {
                statement: "rerun_source_read",
                column: "class_registered",
                declared: "bool",
                rule: BOOL_RULE,
            })
        );
        let shown = match refused {
            Err(refusal) => refusal.to_string(),
            Ok(row) => panic!("the cell breaks its rule, got {row:?}"),
        };
        assert!(
            shown.contains("column `class_registered` is bool"),
            "{shown}"
        );
    }

    #[test]
    fn a_row_that_fits_has_one_value_per_column() {
        let mut cells = vec![SqlTextValue::Null; 19];
        cells[0] = text("t");
        cells[1] = text("TYPESCRIPT");
        let row = caller_row(&read_statement(), CallerRowArg { cells }).expect("the row fits");
        assert_eq!(row.values().len(), 19);
        assert_eq!(row.values()[0], StatementValue::Bool(true));
        assert_eq!(row.values()[2], StatementValue::Null(CallerType::Text));
    }
}
