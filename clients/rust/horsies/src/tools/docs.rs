//! `horsies get-docs`: fetch the docs of this release for AI agents (Python
//! `horsies get-docs`, `clients/python/horsies/core/docs_fetcher.py`).
//!
//! The docs are those of the release tag `v<crate version>`; `--ref main`
//! fetches the `main` branch instead, which can differ from this version.
//! Only the pages that a Rust user reads are kept: every page under `rust/`
//! (the API of the crate) and every page with `section: shared` (behaviour
//! and guarantees of the core, for both clients). The Python API pages and
//! the site pages are dropped. `website/public/rust/llms.txt` is written as
//! `llms.txt`.
//!
//! Two strategies, as in Python: a git sparse checkout, then, when it fails,
//! the tarball of the ref through `curl` and `tar`. Each subprocess is
//! bounded in time. The output folder is replaced only after the new copy is
//! complete.

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::time::{Duration, Instant};

use clap::{Args, ValueEnum};

const REPO_URL: &str = "https://github.com/suleymanozkeskin/horsies";
const DEFAULT_OUTPUT_DIR: &str = ".horsies-docs";
const DOCS_REPO_PATH: &str = "website/src/content/docs";
const LLMS_REPO_PATH: &str = "website/public/rust/llms.txt";
const MAIN_BRANCH: &str = "main";

/// The longest that one git or curl call runs before it is killed.
const SUBPROCESS_BOUND: Duration = Duration::from_secs(60);
/// The pause between two looks at a running subprocess.
const SUBPROCESS_POLL: Duration = Duration::from_millis(50);
/// The most stderr bytes of a failed subprocess that an error quotes.
const MAX_STDERR_QUOTE_BYTES: u64 = 4096;
/// The deepest folder nesting of the docs tree that the copy walks.
const MAX_DOCS_DEPTH: usize = 8;
/// The most files of the docs tree that the copy reads.
const MAX_DOCS_FILES: usize = 2000;
/// The largest docs page that the copy reads.
const MAX_PAGE_BYTES: u64 = 2 * 1024 * 1024;
/// The most frontmatter lines that the section read looks at.
const MAX_FRONTMATTER_LINES: usize = 64;

/// The arguments of `horsies get-docs`.
#[derive(Debug, Clone, Args)]
pub(super) struct GetDocsArgs {
    /// Output directory.
    #[arg(long, default_value = DEFAULT_OUTPUT_DIR)]
    output: PathBuf,
    /// Download the main branch docs instead of the release tag that matches
    /// this version (required for a pre-release version).
    #[arg(long = "ref", value_enum)]
    reference: Option<DocsBranch>,
}

/// The branch that `--ref` names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum DocsBranch {
    Main,
}

/// The git ref of the docs.
#[derive(Debug, Clone, PartialEq, Eq)]
enum DocsRef {
    /// The tag of one release, for example `v0.8.0`.
    ReleaseTag(String),
    /// The `main` branch; it can be ahead of this version.
    MainBranch,
}

impl DocsRef {
    fn git_name(&self) -> &str {
        match self {
            Self::ReleaseTag(tag) => tag,
            Self::MainBranch => MAIN_BRANCH,
        }
    }

    fn tarball_url(&self) -> String {
        match self {
            Self::ReleaseTag(tag) => format!("{REPO_URL}/archive/refs/tags/{tag}.tar.gz"),
            Self::MainBranch => format!("{REPO_URL}/archive/refs/heads/{MAIN_BRANCH}.tar.gz"),
        }
    }
}

/// The section that a page declares in its frontmatter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PageSection {
    Shared,
    Python,
    Site,
    /// No `section:` line: a Rust page, or a file that is not a page.
    NotDeclared,
}

/// Why `get-docs` wrote nothing, or did not finish.
#[derive(Debug, thiserror::Error)]
enum DocsError {
    #[error(
        "installed version {version:?} is not a release version, so no release tag matches \
         it. Run get-docs with --ref main to fetch the main branch docs, which can differ from \
         this version."
    )]
    VersionHasNoTag { version: String },
    #[error("{0}")]
    FetchFailed(String),
    #[error("docs source directory not found: {}", .0.display())]
    SourceMissing(PathBuf),
    #[error("the docs tree at {} {reason}", .path.display())]
    TreeBoundExceeded { path: PathBuf, reason: String },
    #[error("{operation} failed for {}: {cause}", .path.display())]
    Io {
        path: PathBuf,
        operation: &'static str,
        cause: std::io::Error,
    },
    #[error("no files were copied to the output directory")]
    NoFiles,
}

/// Run `get-docs` and return its exit status: 0 with the file count, 1 with
/// the cause.
pub(super) fn run(args: &GetDocsArgs) -> ExitCode {
    let reference = match resolve_docs_ref(env!("CARGO_PKG_VERSION"), args.reference) {
        Ok(reference) => reference,
        Err(error) => {
            eprintln!("error: {error}");
            return ExitCode::FAILURE;
        }
    };
    match fetch_docs(&reference, &args.output) {
        Ok(count) => {
            eprintln!("Done. {count} files written to {}/", args.output.display());
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

/// The ref to download (Python `resolve_docs_ref`). `--ref main` selects
/// `main` for any version; otherwise a version `N.N.N` selects the tag
/// `vN.N.N`. The tag is not checked here: a release with no published tag
/// fails in the download. Pure.
///
/// Errors: [`DocsError::VersionHasNoTag`] for any other version (`0.8.0-rc.1`,
/// `0.1.0-alpha.36`).
fn resolve_docs_ref(version: &str, branch: Option<DocsBranch>) -> Result<DocsRef, DocsError> {
    match (branch, is_release_version(version)) {
        (Some(DocsBranch::Main), _) => Ok(DocsRef::MainBranch),
        (None, true) => Ok(DocsRef::ReleaseTag(format!("v{version}"))),
        (None, false) => Err(DocsError::VersionHasNoTag {
            version: version.to_owned(),
        }),
    }
}

/// Whether `version` is three dot-separated runs of ASCII digits. Pure.
fn is_release_version(version: &str) -> bool {
    let parts: Vec<&str> = version.split('.').collect();
    parts.len() == 3
        && parts
            .iter()
            .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
}

/// Fetch the docs of `reference` into `output`. Returns the file count.
///
/// Errors: [`DocsError`]. Until the copy is complete, an existing `output`
/// is kept; a failure of the final swap names its path.
fn fetch_docs(reference: &DocsRef, output: &Path) -> Result<usize, DocsError> {
    let workspace = tempfile::Builder::new()
        .prefix("horsies-docs-")
        .tempdir()
        .map_err(|cause| DocsError::Io {
            path: std::env::temp_dir(),
            operation: "create a temporary folder",
            cause,
        })?;
    eprintln!(
        "Fetching docs of {} via git sparse checkout...",
        reference.git_name()
    );
    let checkout = match sparse_checkout(reference, workspace.path()) {
        Ok(root) => root,
        Err(error) => {
            eprintln!("Git sparse checkout failed ({error}), falling back to tarball...");
            tarball_checkout(reference, workspace.path())?
        }
    };
    assemble_output(
        &checkout.join(DOCS_REPO_PATH),
        &checkout.join(LLMS_REPO_PATH),
        output,
    )
}

/// A sparse checkout of the docs paths of `reference` under `workspace`.
/// Returns the checkout root.
///
/// Errors: [`DocsError::FetchFailed`] for a git call that fails, cannot
/// start, or runs past [`SUBPROCESS_BOUND`].
fn sparse_checkout(reference: &DocsRef, workspace: &Path) -> Result<PathBuf, DocsError> {
    let clone = workspace.join("repo");
    let repository = format!("{REPO_URL}.git");
    let clone_text = clone.to_string_lossy().into_owned();
    run_bounded(
        "git clone",
        Command::new("git")
            .args([
                "clone",
                "--filter=blob:none",
                "--no-checkout",
                "--depth=1",
                "--branch",
                reference.git_name(),
                repository.as_str(),
                clone_text.as_str(),
            ])
            .current_dir(workspace),
        workspace,
    )?;
    run_bounded(
        "git sparse-checkout init",
        Command::new("git")
            .args(["sparse-checkout", "init", "--cone"])
            .current_dir(&clone),
        workspace,
    )?;
    run_bounded(
        "git sparse-checkout set",
        Command::new("git")
            .args(["sparse-checkout", "set", DOCS_REPO_PATH, LLMS_REPO_PATH])
            .current_dir(&clone),
        workspace,
    )?;
    run_bounded(
        "git checkout",
        Command::new("git")
            .args(["checkout", reference.git_name()])
            .current_dir(&clone),
        workspace,
    )?;
    Ok(clone)
}

/// The tarball of `reference`, downloaded with `curl` and unpacked with
/// `tar` under `workspace`. Returns the one top-level folder of the archive.
///
/// Errors: [`DocsError::FetchFailed`] for a download or unpack that fails,
/// and for an archive without exactly one top-level folder;
/// [`DocsError::Io`] when the unpack folder cannot be made or read.
fn tarball_checkout(reference: &DocsRef, workspace: &Path) -> Result<PathBuf, DocsError> {
    let archive = workspace.join("repo.tar.gz");
    let unpacked = workspace.join("unpacked");
    let archive_text = archive.to_string_lossy().into_owned();
    let unpacked_text = unpacked.to_string_lossy().into_owned();
    let max_time = SUBPROCESS_BOUND.as_secs().to_string();
    let url = reference.tarball_url();
    run_bounded(
        "curl",
        Command::new("curl").args([
            "--fail",
            "--silent",
            "--show-error",
            "--location",
            "--max-time",
            max_time.as_str(),
            "--output",
            archive_text.as_str(),
            url.as_str(),
        ]),
        workspace,
    )?;
    fs::create_dir(&unpacked).map_err(|cause| DocsError::Io {
        path: unpacked.clone(),
        operation: "create the unpack folder",
        cause,
    })?;
    run_bounded(
        "tar",
        Command::new("tar").args(["-xzf", archive_text.as_str(), "-C", unpacked_text.as_str()]),
        workspace,
    )?;
    let read_error = |cause| DocsError::Io {
        path: unpacked.clone(),
        operation: "read the unpack folder",
        cause,
    };
    let mut tops: Vec<PathBuf> = Vec::new();
    for entry in fs::read_dir(&unpacked).map_err(read_error)? {
        tops.push(entry.map_err(read_error)?.path());
        if tops.len() > 1 {
            break;
        }
    }
    match tops.as_slice() {
        [top] if top.is_dir() => Ok(top.clone()),
        _ => Err(DocsError::FetchFailed(format!(
            "unexpected tarball layout from {url}: the archive does not hold exactly one \
             top-level folder"
        ))),
    }
}

/// Run `command` with stdin and stdout closed and stderr in a file of
/// `workspace`, at most [`SUBPROCESS_BOUND`]. A call past the bound is
/// killed.
///
/// Errors: [`DocsError::FetchFailed`] naming `name`, with the exit status
/// and the start of its stderr, or the cause of a start or wait failure.
fn run_bounded(name: &str, command: &mut Command, workspace: &Path) -> Result<(), DocsError> {
    let failed = |cause: String| DocsError::FetchFailed(format!("{name} failed: {cause}"));
    let stderr_path = workspace.join("stderr.txt");
    let stderr = fs::File::create(&stderr_path).map_err(|cause| failed(cause.to_string()))?;
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(stderr)
        .spawn()
        .map_err(|cause| failed(cause.to_string()))?;
    let deadline = Instant::now() + SUBPROCESS_BOUND;
    loop {
        match child.try_wait() {
            Ok(Some(status)) if status.success() => return Ok(()),
            Ok(Some(status)) => {
                return Err(failed(format!(
                    "exit {status}: {}",
                    stderr_quote(&stderr_path)
                )))
            }
            Ok(None) if Instant::now() >= deadline => {
                let _killed = child.kill();
                let _reaped = child.wait();
                return Err(failed(format!(
                    "timed out after {}s",
                    SUBPROCESS_BOUND.as_secs()
                )));
            }
            Ok(None) => std::thread::sleep(SUBPROCESS_POLL),
            Err(cause) => {
                let _killed = child.kill();
                return Err(failed(cause.to_string()));
            }
        }
    }
}

/// The start of the stderr file of a failed call, trimmed; a file that
/// cannot be read is named as such.
fn stderr_quote(path: &Path) -> String {
    let mut text = String::new();
    match fs::File::open(path)
        .and_then(|file| file.take(MAX_STDERR_QUOTE_BYTES).read_to_string(&mut text))
    {
        Ok(_read) => text.trim().to_owned(),
        Err(cause) => format!("(stderr unreadable: {cause})"),
    }
}

/// Copy the kept pages of `docs` and the file `llms` into `output`: first
/// into `<output>.tmp`, then the old `output` is removed and the new copy
/// takes its name. Returns the file count.
///
/// Errors: [`DocsError::SourceMissing`], [`DocsError::TreeBoundExceeded`],
/// [`DocsError::Io`], [`DocsError::NoFiles`]. A failure before the swap
/// leaves `output` as it was.
fn assemble_output(docs: &Path, llms: &Path, output: &Path) -> Result<usize, DocsError> {
    if !docs.is_dir() {
        return Err(DocsError::SourceMissing(docs.to_path_buf()));
    }
    let staging = staging_path(output);
    remove_folder_if_present(&staging)?;
    let mut copied = 0usize;
    let mut seen = 0usize;
    copy_kept_pages(docs, docs, &staging, 0, &mut seen, &mut copied)?;
    if llms.is_file() {
        create_folder(&staging)?;
        let _bytes = fs::copy(llms, staging.join("llms.txt")).map_err(|cause| DocsError::Io {
            path: llms.to_path_buf(),
            operation: "copy",
            cause,
        })?;
        copied += 1;
    }
    if copied == 0 {
        return Err(DocsError::NoFiles);
    }
    remove_folder_if_present(output)?;
    fs::rename(&staging, output).map_err(|cause| DocsError::Io {
        path: staging.clone(),
        operation: "move the new copy into place",
        cause,
    })?;
    Ok(copied)
}

/// `<output>.tmp` beside `output`. Pure.
fn staging_path(output: &Path) -> PathBuf {
    let mut name = output
        .file_name()
        .map_or_else(|| DEFAULT_OUTPUT_DIR.into(), ToOwned::to_owned);
    name.push(".tmp");
    output.with_file_name(name)
}

/// Copy the kept pages under `folder` (a folder of the docs tree `root`)
/// into the same relative paths under `target`, walking at most
/// [`MAX_DOCS_DEPTH`] folders deep and reading at most [`MAX_DOCS_FILES`]
/// files.
///
/// Errors: [`DocsError::TreeBoundExceeded`], [`DocsError::Io`].
fn copy_kept_pages(
    root: &Path,
    folder: &Path,
    target: &Path,
    depth: usize,
    seen: &mut usize,
    copied: &mut usize,
) -> Result<(), DocsError> {
    if depth > MAX_DOCS_DEPTH {
        return Err(DocsError::TreeBoundExceeded {
            path: folder.to_path_buf(),
            reason: format!("nests deeper than {MAX_DOCS_DEPTH} folders"),
        });
    }
    let read_error = |cause| DocsError::Io {
        path: folder.to_path_buf(),
        operation: "read the folder",
        cause,
    };
    let mut entries: Vec<PathBuf> = Vec::new();
    for entry in fs::read_dir(folder).map_err(read_error)? {
        entries.push(entry.map_err(read_error)?.path());
    }
    entries.sort();
    for path in entries {
        if path.is_dir() {
            copy_kept_pages(root, &path, target, depth + 1, seen, copied)?;
            continue;
        }
        *seen += 1;
        if *seen > MAX_DOCS_FILES {
            return Err(DocsError::TreeBoundExceeded {
                path,
                reason: format!("holds more than {MAX_DOCS_FILES} files"),
            });
        }
        let relative = path
            .strip_prefix(root)
            .expect("a walked path is under the root of the walk")
            .to_path_buf();
        let bytes = read_page(&path)?;
        if is_kept_page(&relative, &bytes) {
            let destination = target.join(&relative);
            if let Some(parent) = destination.parent() {
                create_folder(parent)?;
            }
            fs::write(&destination, &bytes).map_err(|cause| DocsError::Io {
                path: destination.clone(),
                operation: "write",
                cause,
            })?;
            *copied += 1;
        }
    }
    Ok(())
}

/// The bytes of the page at `path`, at most [`MAX_PAGE_BYTES`].
///
/// Errors: [`DocsError::Io`]; [`DocsError::TreeBoundExceeded`] for a larger
/// file.
fn read_page(path: &Path) -> Result<Vec<u8>, DocsError> {
    let mut bytes = Vec::new();
    let _read = fs::File::open(path)
        .and_then(|file| file.take(MAX_PAGE_BYTES + 1).read_to_end(&mut bytes))
        .map_err(|cause| DocsError::Io {
            path: path.to_path_buf(),
            operation: "read",
            cause,
        })?;
    match u64::try_from(bytes.len()) {
        Ok(length) if length <= MAX_PAGE_BYTES => Ok(bytes),
        Ok(_) | Err(_) => Err(DocsError::TreeBoundExceeded {
            path: path.to_path_buf(),
            reason: format!("is larger than {MAX_PAGE_BYTES} bytes"),
        }),
    }
}

/// Whether a Rust user reads the page at `relative` (a path inside the docs
/// tree): a page under `rust/`, or a page with `section: shared`. Pure.
fn is_kept_page(relative: &Path, bytes: &[u8]) -> bool {
    match relative.starts_with("rust") {
        true => true,
        false => match page_section(&String::from_utf8_lossy(bytes)) {
            PageSection::Shared => true,
            PageSection::Python | PageSection::Site | PageSection::NotDeclared => false,
        },
    }
}

/// The `section:` of the frontmatter of `text`: the lines between a first
/// line `---` and the next `---`, at most [`MAX_FRONTMATTER_LINES`] of them.
/// Pure.
fn page_section(text: &str) -> PageSection {
    let mut lines = text.lines();
    if lines.next().map(str::trim_end) != Some("---") {
        return PageSection::NotDeclared;
    }
    for line in lines.take(MAX_FRONTMATTER_LINES) {
        let line = line.trim_end();
        if line == "---" {
            return PageSection::NotDeclared;
        }
        if let Some(value) = line.strip_prefix("section:") {
            return match value.trim() {
                "shared" => PageSection::Shared,
                "python" => PageSection::Python,
                "site" => PageSection::Site,
                _other => PageSection::NotDeclared,
            };
        }
    }
    PageSection::NotDeclared
}

fn create_folder(path: &Path) -> Result<(), DocsError> {
    fs::create_dir_all(path).map_err(|cause| DocsError::Io {
        path: path.to_path_buf(),
        operation: "create the folder",
        cause,
    })
}

fn remove_folder_if_present(path: &Path) -> Result<(), DocsError> {
    match path.exists() {
        false => Ok(()),
        true => fs::remove_dir_all(path).map_err(|cause| DocsError::Io {
            path: path.to_path_buf(),
            operation: "remove the old folder",
            cause,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_release_version_asks_for_its_tag() {
        assert_eq!(
            resolve_docs_ref("0.8.0", None).unwrap(),
            DocsRef::ReleaseTag("v0.8.0".to_owned())
        );
        assert_eq!(
            DocsRef::ReleaseTag("v0.8.0".to_owned()).tarball_url(),
            "https://github.com/suleymanozkeskin/horsies/archive/refs/tags/v0.8.0.tar.gz"
        );
    }

    #[test]
    fn a_pre_release_version_has_no_tag_unless_main_is_asked_for() {
        for version in ["0.1.0-alpha.36", "0.8.0-rc.1", "0.8", "0.8.0.1", "v0.8.0"] {
            let refused = resolve_docs_ref(version, None).unwrap_err();
            assert!(
                matches!(refused, DocsError::VersionHasNoTag { .. }),
                "{version}"
            );
            assert!(refused.to_string().contains("--ref main"));
            assert_eq!(
                resolve_docs_ref(version, Some(DocsBranch::Main)).unwrap(),
                DocsRef::MainBranch
            );
        }
    }

    #[test]
    fn the_section_is_read_from_the_frontmatter_only() {
        assert_eq!(
            page_section("---\ntitle: A\nsection: shared\n---\n# A\n"),
            PageSection::Shared
        );
        assert_eq!(
            page_section("---\nsection: python\n---\n"),
            PageSection::Python
        );
        assert_eq!(page_section("---\nsection: site\n---\n"), PageSection::Site);
        assert_eq!(
            page_section("---\ntitle: A\n---\nsection: shared\n"),
            PageSection::NotDeclared
        );
        assert_eq!(
            page_section("# no frontmatter\nsection: shared\n"),
            PageSection::NotDeclared
        );
    }

    #[test]
    fn rust_pages_and_shared_pages_are_kept() {
        let shared = b"---\nsection: shared\n---\n";
        let python = b"---\nsection: python\n---\n";
        assert!(is_kept_page(
            Path::new("rust/tasks/errors.md"),
            b"---\ntitle: E\n---\n"
        ));
        assert!(is_kept_page(Path::new("concepts/architecture.md"), shared));
        assert!(!is_kept_page(Path::new("tasks/defining-tasks.md"), python));
        assert!(!is_kept_page(
            Path::new("changelog.md"),
            b"---\nsection: site\n---\n"
        ));
        assert!(!is_kept_page(Path::new("rustacean.md"), python));
    }

    #[test]
    fn the_output_holds_the_kept_pages_and_the_rust_llms_file() {
        let source = tempfile::tempdir().unwrap();
        let docs = source.path().join("docs");
        fs::create_dir_all(docs.join("rust/tasks")).unwrap();
        fs::create_dir_all(docs.join("tasks")).unwrap();
        fs::write(docs.join("rust/tasks/errors.md"), "---\ntitle: E\n---\n").unwrap();
        fs::write(
            docs.join("tasks/retry-policy.md"),
            "---\nsection: shared\n---\n",
        )
        .unwrap();
        fs::write(
            docs.join("tasks/defining-tasks.md"),
            "---\nsection: python\n---\n",
        )
        .unwrap();
        let llms = source.path().join("llms.txt");
        fs::write(&llms, "rust pages").unwrap();
        let out = tempfile::tempdir().unwrap();
        let output = out.path().join(".horsies-docs");
        fs::create_dir_all(&output).unwrap();
        fs::write(output.join("stale.md"), "old").unwrap();

        let count = assemble_output(&docs, &llms, &output).unwrap();

        assert_eq!(count, 3);
        assert!(output.join("rust/tasks/errors.md").is_file());
        assert!(output.join("tasks/retry-policy.md").is_file());
        assert!(!output.join("tasks/defining-tasks.md").exists());
        assert!(!output.join("stale.md").exists());
        assert_eq!(
            fs::read_to_string(output.join("llms.txt")).unwrap(),
            "rust pages"
        );
        assert!(!staging_path(&output).exists());
    }

    #[test]
    fn a_missing_docs_tree_keeps_the_old_output() {
        let out = tempfile::tempdir().unwrap();
        let output = out.path().join(".horsies-docs");
        fs::create_dir_all(&output).unwrap();
        fs::write(output.join("kept.md"), "old").unwrap();

        let refused = assemble_output(
            &out.path().join("missing"),
            &out.path().join("llms.txt"),
            &output,
        )
        .unwrap_err();

        assert!(matches!(refused, DocsError::SourceMissing(_)));
        assert!(output.join("kept.md").is_file());
    }
}
