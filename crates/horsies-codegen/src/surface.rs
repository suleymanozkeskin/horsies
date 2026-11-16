//! The Python surface of the extension module `horsies._core`, as data.
//!
//! The classes and functions here are defined with PyO3 in
//! `crates/horsies-python`. PyO3 attribute values must be literals, so this
//! table repeats their names and signatures. A change to the extension
//! changes this table in the same commit; a Python test compares the
//! generated stub with the built module. Where a value is a catalog code,
//! the type comes from the core catalog itself.

use horsies_core::catalog::{CodeCatalog, CodeEnum};
use horsies_core::BrokerErrorCode;

/// The type text of an annotation.
#[derive(Clone, Copy, Debug)]
pub enum TypeText {
    /// Python type text, written as it is.
    Plain(&'static str),
    /// `Literal[...]` of every wire value of a catalog.
    WireValuesOf(CodeCatalog),
}

/// An exception class.
#[derive(Clone, Copy, Debug)]
pub struct StubException {
    pub name: &'static str,
    pub base: &'static str,
    pub doc: &'static str,
}

/// A parameter of a function or method, after `self`.
#[derive(Clone, Copy, Debug)]
pub struct StubParameter {
    pub name: &'static str,
    pub annotation: TypeText,
}

/// A function, or a method that takes `self`.
#[derive(Clone, Copy, Debug)]
pub struct StubFunction {
    pub name: &'static str,
    pub parameters: &'static [StubParameter],
    pub returns: TypeText,
    pub doc: &'static str,
}

/// A read-only attribute of a class.
#[derive(Clone, Copy, Debug)]
pub struct StubProperty {
    pub name: &'static str,
    pub annotation: TypeText,
}

/// A final class.
#[derive(Clone, Copy, Debug)]
pub struct StubClass {
    pub name: &'static str,
    pub doc: &'static str,
    /// The parameters of `__init__`, or `NoConstructor` when Python cannot
    /// build the class.
    pub constructor: Constructor,
    pub properties: &'static [StubProperty],
    pub methods: &'static [StubFunction],
}

/// Whether Python can build a class.
#[derive(Clone, Copy, Debug)]
pub enum Constructor {
    Parameters(&'static [StubParameter]),
    NoConstructor,
}

/// The whole module.
#[derive(Clone, Copy, Debug)]
pub struct ModuleSurface {
    pub exceptions: &'static [StubException],
    pub classes: &'static [StubClass],
    pub functions: &'static [StubFunction],
}

/// The outcome type of an operation that returns `record` or a
/// `BrokerFailure`.
const PING_OUTCOME: &str = "DatabasePingRecord | BrokerFailure";

/// The surface of `horsies._core`.
pub const CORE_MODULE: ModuleSurface = ModuleSurface {
    exceptions: &[
        StubException {
            name: "CoreError",
            base: "Exception",
            doc: "Base of the errors raised at the core boundary.",
        },
        StubException {
            name: "ForkedAfterCoreStart",
            base: "CoreError",
            doc: "This process was forked without exec after the core started in its parent.\n\nargs: (core_pid, this_pid, advice).",
        },
        StubException {
            name: "CoreStopped",
            base: "CoreError",
            doc: "A core background task panicked, so the core is stopped in this process.\n\nargs: (subject, cause) of the first panic.",
        },
        StubException {
            name: "CoreStartFailed",
            base: "CoreError",
            doc: "The core runtime or its log forwarder could not start. args: (cause,).",
        },
        StubException {
            name: "CoreCallTimedOut",
            base: "CoreError",
            doc: "A blocking call did not end within its bound.\n\nargs: (subject, bound_seconds, state_after); state_after is 'no_change'\nfor a read and 'outcome_unknown' for a write.",
        },
        StubException {
            name: "TooManyPendingCalls",
            base: "CoreError",
            doc: "The event loop already has the most core calls in flight. args: (subject, bound).",
        },
        StubException {
            name: "CoreDefect",
            base: "BaseException",
            doc: "The client sent a value that the core refuses: a defect of the client.",
        },
        StubException {
            name: "ManifestRefused",
            base: "CoreDefect",
            doc: "The core refused a manifest. args: (field_path, cause).",
        },
        StubException {
            name: "PanicException",
            base: "BaseException",
            doc: "Rust code called from Python panicked.",
        },
    ],
    classes: &[
        StubClass {
            name: "BrokerFailure",
            doc: "An expected failure of a broker operation.",
            constructor: Constructor::NoConstructor,
            properties: &[
                StubProperty {
                    name: "code",
                    annotation: TypeText::WireValuesOf(<BrokerErrorCode as CodeEnum>::CATALOG),
                },
                StubProperty {
                    name: "message",
                    annotation: TypeText::Plain("str"),
                },
                StubProperty {
                    name: "retryable",
                    annotation: TypeText::Plain("bool"),
                },
            ],
            methods: &[],
        },
        StubClass {
            name: "DatabasePingRecord",
            doc: "The round trip of one database ping.",
            constructor: Constructor::NoConstructor,
            properties: &[StubProperty {
                name: "latency_ms",
                annotation: TypeText::Plain("float"),
            }],
            methods: &[],
        },
        StubClass {
            name: "CoreHandle",
            doc: "The core handle of one producer manifest. Building it starts nothing.",
            constructor: Constructor::Parameters(&[StubParameter {
                name: "manifest_json",
                annotation: TypeText::Plain("str"),
            }]),
            properties: &[StubProperty {
                name: "manifest_version",
                annotation: TypeText::Plain("int"),
            }],
            methods: &[
                StubFunction {
                    name: "ping_database",
                    parameters: &[],
                    returns: TypeText::Plain(PING_OUTCOME),
                    doc: "Ping the database.",
                },
                StubFunction {
                    name: "ping_database_async",
                    parameters: &[],
                    returns: TypeText::Plain("asyncio.Future[DatabasePingRecord | BrokerFailure]"),
                    doc: "The awaitable form of ping_database.",
                },
            ],
        },
    ],
    functions: &[
        StubFunction {
            name: "core_state",
            parameters: &[],
            returns: TypeText::Plain("Literal['not_started', 'running', 'stopped', 'inherited']"),
            doc: "The state of the core in this process. Starts nothing.",
        },
        StubFunction {
            name: "refresh_log_levels",
            parameters: &[],
            returns: TypeText::Plain("None"),
            doc: "Read the effective level of every horsies logger again.",
        },
    ],
};
