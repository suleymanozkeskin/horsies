//! Partition DDL: the one statement text that the core builds at run time.
//!
//! `ALTER TABLE <parent> DETACH PARTITION <leaf> CONCURRENTLY | FINALIZE`
//! cannot run in a function, in a transaction block or in a multi-statement
//! query, so the retention pass sends it itself, in autocommit, on the
//! session that holds the leaf's session lock. Its text names two relations,
//! so it cannot be a constant. The text is safe by construction:
//! - only [`SqlIdentifier::parse`] builds an identifier, with the rule of
//!   `is_safe_identifier` (an unquoted lowercase identifier of at most 63
//!   bytes);
//! - only the two detach forms of [`PartitionDdl`] exist. There is no free
//!   text form.
//!
//! [`crate::db::PooledConnection::run_partition_ddl`] sends it.

use std::fmt;

/// The longest identifier that PostgreSQL keeps without truncation
/// (`NAMEDATALEN` - 1).
pub const MAX_IDENTIFIER_BYTES: usize = 63;

/// An unquoted SQL identifier: a lowercase ASCII letter, then lowercase ASCII
/// letters, digits and `_`, at most [`MAX_IDENTIFIER_BYTES`] bytes. Such a
/// name means the same relation quoted or unquoted, so it goes into a
/// statement text as it is.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SqlIdentifier(String);

/// Why a text is not an unquoted SQL identifier.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SqlIdentifierError {
    #[error("an SQL identifier must not be empty")]
    Empty,
    #[error("SQL identifier {text:?} has {bytes} bytes; at most 63 are allowed")]
    TooLong { text: String, bytes: usize },
    #[error("SQL identifier {text:?} must start with a lowercase ASCII letter")]
    BadFirstCharacter { text: String },
    #[error(
        "SQL identifier {text:?} contains {character:?}; only lowercase ASCII letters, digits \
         and `_` are allowed"
    )]
    BadCharacter { text: String, character: char },
}

impl SqlIdentifier {
    /// Checks `text` and keeps it.
    ///
    /// Errors: [`SqlIdentifierError`] names the rule that `text` breaks.
    pub fn parse(text: &str) -> Result<SqlIdentifier, SqlIdentifierError> {
        let Some(first) = text.chars().next() else {
            return Err(SqlIdentifierError::Empty);
        };
        if text.len() > MAX_IDENTIFIER_BYTES {
            return Err(SqlIdentifierError::TooLong {
                text: text.to_owned(),
                bytes: text.len(),
            });
        }
        if !first.is_ascii_lowercase() {
            return Err(SqlIdentifierError::BadFirstCharacter {
                text: text.to_owned(),
            });
        }
        match text.chars().find(|character| {
            !(character.is_ascii_lowercase() || character.is_ascii_digit() || *character == '_')
        }) {
            Some(character) => Err(SqlIdentifierError::BadCharacter {
                text: text.to_owned(),
                character,
            }),
            None => Ok(SqlIdentifier(text.to_owned())),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SqlIdentifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// The partition DDL that the core sends: the two forms of a leaf detach.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartitionDdl {
    /// `ALTER TABLE <parent> DETACH PARTITION <leaf> CONCURRENTLY`: the
    /// detach of an attached leaf.
    DetachConcurrently {
        parent: SqlIdentifier,
        leaf: SqlIdentifier,
    },
    /// `ALTER TABLE <parent> DETACH PARTITION <leaf> FINALIZE`: the end of a
    /// concurrent detach that a cancel or a crash left pending.
    DetachFinalize {
        parent: SqlIdentifier,
        leaf: SqlIdentifier,
    },
}

impl PartitionDdl {
    /// The name of the statement in errors and logs.
    pub fn name(&self) -> &'static str {
        match self {
            PartitionDdl::DetachConcurrently { .. } => "detach_partition_concurrently",
            PartitionDdl::DetachFinalize { .. } => "detach_partition_finalize",
        }
    }

    /// The statement text.
    pub fn sql(&self) -> String {
        match self {
            PartitionDdl::DetachConcurrently { parent, leaf } => {
                format!("ALTER TABLE {parent} DETACH PARTITION {leaf} CONCURRENTLY")
            }
            PartitionDdl::DetachFinalize { parent, leaf } => {
                format!("ALTER TABLE {parent} DETACH PARTITION {leaf} FINALIZE")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identifier(text: &str) -> SqlIdentifier {
        SqlIdentifier::parse(text).expect("a valid identifier")
    }

    #[test]
    fn a_lowercase_identifier_is_kept_as_it_is() {
        let parsed = identifier("horsies_task_history_standard_30d_2026_08_06");
        assert_eq!(
            parsed.as_str(),
            "horsies_task_history_standard_30d_2026_08_06"
        );
        assert_eq!(parsed.to_string(), parsed.as_str());
        assert!(SqlIdentifier::parse(&"a".repeat(MAX_IDENTIFIER_BYTES)).is_ok());
    }

    #[test]
    fn each_broken_rule_is_named() {
        assert_eq!(SqlIdentifier::parse(""), Err(SqlIdentifierError::Empty));
        assert!(matches!(
            SqlIdentifier::parse(&"a".repeat(MAX_IDENTIFIER_BYTES + 1)),
            Err(SqlIdentifierError::TooLong { bytes: 64, .. })
        ));
        for text in ["_leaf", "1leaf", "Leaf"] {
            assert!(
                matches!(
                    SqlIdentifier::parse(text),
                    Err(SqlIdentifierError::BadFirstCharacter { .. })
                ),
                "{text}"
            );
        }
        for (text, character) in [
            ("leaf name", ' '),
            ("leaf;drop", ';'),
            ("leaf\"x", '"'),
            ("leafX", 'X'),
            ("leaf-1", '-'),
            ("lеaf", 'е'),
        ] {
            assert_eq!(
                SqlIdentifier::parse(text),
                Err(SqlIdentifierError::BadCharacter {
                    text: text.to_owned(),
                    character,
                }),
                "{text}"
            );
        }
    }

    #[test]
    fn each_detach_form_renders_its_statement() {
        let parent = identifier("horsies_task_history_standard_30d");
        let leaf = identifier("horsies_task_history_standard_30d_2026_08_06");
        let concurrently = PartitionDdl::DetachConcurrently {
            parent: parent.clone(),
            leaf: leaf.clone(),
        };
        let finalize = PartitionDdl::DetachFinalize { parent, leaf };
        assert_eq!(
            concurrently.sql(),
            "ALTER TABLE horsies_task_history_standard_30d DETACH PARTITION \
             horsies_task_history_standard_30d_2026_08_06 CONCURRENTLY"
        );
        assert_eq!(
            finalize.sql(),
            "ALTER TABLE horsies_task_history_standard_30d DETACH PARTITION \
             horsies_task_history_standard_30d_2026_08_06 FINALIZE"
        );
        assert_eq!(concurrently.name(), "detach_partition_concurrently");
        assert_eq!(finalize.name(), "detach_partition_finalize");
    }
}
