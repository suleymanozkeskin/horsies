//! Marker files: how a task in a worker child and the test that sent it
//! coordinate across processes (Python `tests/parity/snapshot/apps/whole_run_app.py`,
//! `_first_call`, `_wait_for_marker`).
//!
//! The test creates the directory and gives its path to the worker in
//! [`MARKER_DIR_ENV`]; every child inherits it. A marker is a file in that
//! directory. An exclusive create tells the first call from a later one, so
//! a marker counts attempts across children and retries.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use uuid::Uuid;

/// The variable that names the marker directory of a worker and its
/// children.
pub const MARKER_DIR_ENV: &str = "HORSIES_RUST_E2E_MARKER_DIR";

/// The most attempts that [`next_attempt`] counts: a task has at most 20
/// retries, so 21 runs.
pub const MAX_ATTEMPTS: u32 = 21;

/// The interval of [`wait_for_marker`].
const MARKER_POLL: Duration = Duration::from_millis(50);

/// Why a marker operation failed in a task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarkerError {
    /// The worker was started without [`MARKER_DIR_ENV`].
    DirectoryNotSet,
    /// A file operation failed; the cause names the path.
    Io(String),
    /// [`next_attempt`] found [`MAX_ATTEMPTS`] markers already.
    TooManyAttempts,
}

impl std::fmt::Display for MarkerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DirectoryNotSet => write!(formatter, "{MARKER_DIR_ENV} is not set"),
            Self::Io(cause) => formatter.write_str(cause),
            Self::TooManyAttempts => write!(formatter, "more than {MAX_ATTEMPTS} attempts"),
        }
    }
}

/// The path of marker `name` in the directory of [`MARKER_DIR_ENV`].
pub fn marker_path(name: &str) -> Result<PathBuf, MarkerError> {
    std::env::var_os(MARKER_DIR_ENV)
        .filter(|value| !value.is_empty())
        .map(|directory| Path::new(&directory).join(name))
        .ok_or(MarkerError::DirectoryNotSet)
}

/// Create marker `name` with `content`; `Ok(false)` when it exists already.
pub fn create_once(name: &str, content: &str) -> Result<bool, MarkerError> {
    let path = marker_path(name)?;
    match OpenOptions::new().write(true).create_new(true).open(&path) {
        Ok(mut file) => file
            .write_all(content.as_bytes())
            .map(|()| true)
            .map_err(|error| MarkerError::Io(format!("write {}: {error}", path.display()))),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(error) => Err(MarkerError::Io(format!(
            "create {}: {error}",
            path.display()
        ))),
    }
}

/// Whether marker `name` exists.
pub fn exists(name: &str) -> Result<bool, MarkerError> {
    Ok(marker_path(name)?.exists())
}

/// The attempt number of this call for `token`: 1 for the first call, then
/// 2, 3, ... Each call creates the marker `<token>-attempt-<n>`.
pub fn next_attempt(token: &str) -> Result<u32, MarkerError> {
    for attempt in 1..=MAX_ATTEMPTS {
        if create_once(&format!("{token}-attempt-{attempt}"), "")? {
            return Ok(attempt);
        }
    }
    Err(MarkerError::TooManyAttempts)
}

/// Append `line` to marker `name`, which is created when absent.
pub fn append_line(name: &str, line: &str) -> Result<(), MarkerError> {
    let path = marker_path(name)?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|error| MarkerError::Io(format!("open {}: {error}", path.display())))?;
    writeln!(file, "{line}")
        .map_err(|error| MarkerError::Io(format!("append to {}: {error}", path.display())))
}

/// Wait until marker `name` exists, at most `limit`. `Ok(false)` when the
/// limit passed first.
pub async fn wait_for_marker(name: &str, limit: Duration) -> Result<bool, MarkerError> {
    let path = marker_path(name)?;
    let deadline = Instant::now() + limit;
    loop {
        if path.exists() {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(MARKER_POLL).await;
    }
}

/// The wall clock in milliseconds since 1970, for marker lines that a test
/// compares across processes.
pub fn unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis())
        .unwrap_or(0)
}

/// A marker directory of one test, removed when dropped.
#[derive(Debug)]
pub struct MarkerDir {
    path: PathBuf,
}

impl MarkerDir {
    /// Create a new empty directory under the system temporary directory.
    /// Panics when the create fails.
    pub fn create() -> MarkerDir {
        let path = std::env::temp_dir().join(format!("horsies-rs-e2e-{}", Uuid::new_v4().simple()));
        std::fs::create_dir_all(&path).unwrap_or_else(|error| {
            panic!("create the marker directory {}: {error}", path.display())
        });
        MarkerDir { path }
    }

    /// The directory.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The path of marker `name`.
    pub fn marker(&self, name: &str) -> PathBuf {
        self.path.join(name)
    }

    /// The text of marker `name`, or `None` when it does not exist.
    pub fn read(&self, name: &str) -> Option<String> {
        std::fs::read_to_string(self.marker(name)).ok()
    }

    /// Create marker `name` from the test. Panics when the write fails.
    pub fn write(&self, name: &str, content: &str) {
        let path = self.marker(name);
        std::fs::write(&path, content)
            .unwrap_or_else(|error| panic!("write the marker {}: {error}", path.display()));
    }

    /// The number of markers whose name starts with `prefix`.
    pub fn count(&self, prefix: &str) -> usize {
        std::fs::read_dir(&self.path)
            .map(|entries| {
                entries
                    .filter_map(Result::ok)
                    .filter(|entry| entry.file_name().to_string_lossy().starts_with(prefix))
                    .count()
            })
            .unwrap_or(0)
    }
}

impl Drop for MarkerDir {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.path) {
            eprintln!(
                "the marker directory {} was not removed: {error}",
                self.path.display()
            );
        }
    }
}
