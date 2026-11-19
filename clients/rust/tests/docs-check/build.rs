//! Writes the `rust` code blocks of the docs site and of the Rust skill
//! files as modules of this crate, so that a build of the crate builds every
//! block. The rules of a block and of its markers are in `src/lib.rs`.
//!
//! Output, in `OUT_DIR`: `blocks.rs`, which `src/lib.rs` includes, with one
//! `mod` per block that starts a chain or builds alone, and `blocks/`, with
//! one file per block. A chained block is a module inside the module of the
//! block before it. A page with an extraction error stops the build with one
//! line per error.

mod extract;

fn main() {
    if let Err(errors) = extract::run() {
        for error in &errors {
            eprintln!("{error}");
        }
        std::process::exit(1);
    }
}
