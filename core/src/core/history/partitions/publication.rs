//! Seam between partition lifecycle and the staged-reader publisher.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoaderRepublished {
    pub absent_leaves: Vec<String>,
}
