//! The failure codes of a rerun, as one closed catalog for the clients.

use serde::{Deserialize, Serialize};

/// The failure code of a rerun: in a transaction of the client's own
/// handle, or in a transaction that the caller owns.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_core_derive::CodeCatalog,
)]
#[code_catalog(module = "history", base = "str_enum")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RerunErrorCode {
    /// The handle could not connect, could not install the schema, or could
    /// not open its closed pools again. Nothing ran.
    ConnectFailed,
    /// A statement of the rerun's own transaction failed. Nothing committed,
    /// except when the reply to `COMMIT` was lost: then the outcome is
    /// unknown, and the same keyed rerun sent again answers its key replay.
    StatementFailed,
    /// The check of the database's client family could not read the family
    /// row. Nothing was read or written.
    FamilyCheckFailed,
    /// The command breaks a rule of its fields. Nothing ran.
    InvalidCommand,
    /// The policy breaks a rule of its fields. Nothing ran.
    InvalidPolicy,
    /// The retention class of the new task has no class row: register its
    /// period first. Nothing was written.
    ClassNotRegistered,
    /// The caller key is not a valid idempotency key. Nothing was written.
    CallerKeyRefused,
    /// The task id source refused to mint the id of the new task. Nothing
    /// was written.
    IdMintRefused,
    /// The database records another client family, or none. Nothing was
    /// written.
    ClientFamilyMismatch,
    /// The family row holds a value that is not a client family: corrupt
    /// stored data. Nothing was written.
    UnknownStoredFamily,
    /// A row or stored data broke the rerun's contract. Nothing committed;
    /// the same call meets it again.
    ContractViolation,
    /// A statement of the rerun failed in the driver of a transaction that
    /// the caller owns. The caller's transaction is aborted: roll it back.
    DriverFailed,
    /// The caller's connection had no open transaction for a statement of
    /// the rerun, so the statement did not run: without a transaction it
    /// would commit by itself. Begin a transaction first, or roll back a
    /// failed one.
    NoOpenTransaction,
    /// The caller's driver gave a result column in a format other than
    /// text, for example a `pg` client with `binary: true`. The statement
    /// ran: roll the caller's transaction back.
    ResultNotText,
    /// The stored retry list of the source names a custom code that the
    /// task does not declare in the calling app. Nothing was written.
    RetryCodeNotDeclared,
    /// The stored retry list of the source names a custom code, and the
    /// calling app does not register the task, so the code cannot be
    /// checked. Nothing was written.
    TaskNotRegistered,
}
