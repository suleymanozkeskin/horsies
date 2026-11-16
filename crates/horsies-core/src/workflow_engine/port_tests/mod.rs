//! Core tests ported from the Python workflow engine suite.
//!
//! Each file ports one Python test file (or a group of small ones) of the test
//! map; each test names its Python test in its doc comment.

mod support;

mod completion_edges;
mod dataflow;
mod error_handling;
mod handle_reads;
mod joins_and_patterns;
mod node_finalize;
mod output_skipped;
mod pause_resume_cancel;
mod promotion_rows;
mod recovery_cases;
mod recovery_limit_pass;
mod start_rows;
mod subworkflow_lifecycle;
mod subworkflow_matrix;
mod subworkflow_rows;
