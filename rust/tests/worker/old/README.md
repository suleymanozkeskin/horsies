# Old workflow layer tests, kept out of the build

`layer7_subworkflows.rs` holds the end-to-end tests of the old Rust client. They use the old API, so they do not
compile. Cargo builds only `tests/`, so this folder is not built.

Each test is ported to `../tests/` on the harness of the crate (#1754); its row in `../../old-client-test-fates.tsv`
then changes from `TO_PORT` to `PORTED`, and the old file is deleted when it has no test left.
