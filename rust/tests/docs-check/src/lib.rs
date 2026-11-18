//! The `rust` code blocks of the docs site (`website/src/content/docs/rust/`)
//! and of the Rust skill files (`crates/horsies/.agents/skills/horsies/`),
//! built as modules of this crate (Python's twin: `tests/docs_examples/extract.py`).
//!
//! Run: `cargo check -p horsies-docs-check --all-targets`. A block that the
//! client's API does not build fails the check, with the page and the line.
//!
//! # The rules of a block
//!
//! A checked block holds items: `use` lines, functions, types, impls, tasks
//! and definitions. A block of statements does not build at module level:
//! mark it.
//!
//! - No marker: the block is chained. It is a module inside the module of
//!   the chained block before it on the page (`use super::*`), so it sees
//!   the names of the chained blocks above it, and a name that it defines
//!   again hides the one above.
//! - `rust-check: alone`: the block is a module of its own and sees no other
//!   block. The chain goes on after it.
//! - `rust-check: skip KIND -- reason`: the block is not built. The kind and
//!   the reason are required:
//!   - `elided`: the block shortens on purpose (pseudo-code, a signature
//!     shown for reference, a "Wrong / Correct" pair);
//!   - `partial`: the block is correct but uses names that the page does not
//!     define (a fragment of a function above it, a function of another
//!     page);
//!   - `defect`: the block is wrong for the current API: a known docs defect,
//!     to be fixed.
//!
//! A marker is a comment on the line directly above the opening fence, with
//! only blank lines between: `<!-- rust-check: skip partial -- reason -->` in
//! `.md`, `{/* rust-check: skip partial -- reason */}` in `.mdx`. An unknown
//! marker, a skip with no kind or no reason, a marker with no `rust` fence
//! under it, and a fence that is never closed stop the build.
//!
//! The first line of each generated module file names its page and the line
//! offset: an error at line L of the file is at line `L + offset` of the
//! page.

// The blocks are examples: they define names that no other code uses, and
// show values that a program would use.
#![allow(unused, non_snake_case, clippy::all)]

include!(concat!(env!("OUT_DIR"), "/blocks.rs"));
