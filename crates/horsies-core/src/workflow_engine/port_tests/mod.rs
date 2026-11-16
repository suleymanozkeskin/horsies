//! Core tests ported from the Python workflow engine suite.
//!
//! Each file ports one Python test file (or a group of small ones) of the test
//! map; each test names its Python test in its doc comment.

mod support;

mod completion_edges;
mod handle_reads;
mod output_skipped;
mod recovery_limit_pass;
