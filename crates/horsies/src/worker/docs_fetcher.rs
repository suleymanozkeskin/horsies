/// Fetch horsies documentation locally for AI agents.
///
/// Two strategies: git sparse checkout (primary, fast) with
/// HTTP tarball fallback (no git required). Idempotent —
/// run once to download, run again to update.
///
/// Mirrors Python's `horsies/core/docs_fetcher.py`.
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

const REPO_URL: &str = "https://github.com/suleymanozkeskin/horsies";
pub const DEFAULT_OUTPUT_DIR: &str = ".horsies-docs";
const DOCS_REPO_PATH: &str = "website/src/content/docs";
const LLMS_REPO_PATH: &str = "website/public/llms.txt";
const DEFAULT_BRANCH: &str = "main";
const GIT_TIMEOUT_SECONDS: u64 = 60;

/// Error type for docs fetching operations.
#[derive(Debug, thiserror::Error)]
pub enum DocsFetchError {
    #[error("{0}")]
    Failed(String),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

/// Fetch horsies docs into `output_dir`. Returns file count.
pub fn fetch_docs(output_dir: &str) -> Result<usize, DocsFetchError> {
    let output_path = PathBuf::from(output_dir);
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }

    if has_git() {
        print_status("Fetching docs via git sparse checkout...");
        match fetch_via_sparse_checkout(&output_path) {
            Ok(count) => {
                print_status(&format!("Done. {} files written to {}/", count, output_dir));
                return Ok(count);
            }
            Err(e) => {
                print_status(&format!(
                    "Git sparse checkout failed ({}), falling back to tarball...",
                    e
                ));
            }
        }
    }

    print_status("Fetching docs via tarball download...");
    let count = fetch_via_tarball(&output_path)?;
    print_status(&format!("Done. {} files written to {}/", count, output_dir));
    Ok(count)
}

fn has_git() -> bool {
    Command::new("git")
        .args(["sparse-checkout", "--help"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn run_git(args: &[&str], cwd: &Path) -> Result<(), DocsFetchError> {
    let output = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .map_err(|e| DocsFetchError::Failed(format!("git {} failed: {}", args[0], e)))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(DocsFetchError::Failed(format!(
            "git {} failed (exit {}): {}",
            args[0],
            output.status,
            stderr.trim(),
        )));
    }
    Ok(())
}

fn fetch_via_sparse_checkout(output_dir: &Path) -> Result<usize, DocsFetchError> {
    let tmpdir = tempfile::tempdir()
        .map_err(|e| DocsFetchError::Failed(format!("failed to create temp dir: {}", e)))?;
    let clone_dir = tmpdir.path().join("repo");

    let repo_git = format!("{}.git", REPO_URL);
    run_git(
        &[
            "clone",
            "--filter=blob:none",
            "--no-checkout",
            "--depth=1",
            &repo_git,
            clone_dir.to_str().unwrap_or("repo"),
        ],
        tmpdir.path(),
    )?;
    run_git(&["sparse-checkout", "init", "--cone"], &clone_dir)?;
    run_git(
        &["sparse-checkout", "set", DOCS_REPO_PATH, LLMS_REPO_PATH],
        &clone_dir,
    )?;
    run_git(&["checkout", DEFAULT_BRANCH], &clone_dir)?;

    let docs_src = clone_dir.join(DOCS_REPO_PATH);
    let llms_src = clone_dir.join(LLMS_REPO_PATH);
    assemble_output(&docs_src, &llms_src, output_dir)
}

fn fetch_via_tarball(output_dir: &Path) -> Result<usize, DocsFetchError> {
    let tarball_url = format!("{}/archive/refs/heads/{}.tar.gz", REPO_URL, DEFAULT_BRANCH);
    let tmpdir = tempfile::tempdir()
        .map_err(|e| DocsFetchError::Failed(format!("failed to create temp dir: {}", e)))?;
    let tarball_path = tmpdir.path().join("repo.tar.gz");

    // Download using curl (available on all platforms we target).
    let status = Command::new("curl")
        .args([
            "-sL",
            "--max-time",
            &GIT_TIMEOUT_SECONDS.to_string(),
            "-o",
            tarball_path.to_str().unwrap_or("repo.tar.gz"),
            &tarball_url,
        ])
        .status()
        .map_err(|e| DocsFetchError::Failed(format!("curl failed: {}", e)))?;

    if !status.success() {
        return Err(DocsFetchError::Failed(format!(
            "failed to download tarball from {}",
            tarball_url,
        )));
    }

    // Extract using tar.
    let extract_dir = tmpdir.path().join("extracted");
    fs::create_dir_all(&extract_dir)?;
    let tar_status = Command::new("tar")
        .args([
            "xzf",
            tarball_path.to_str().unwrap_or("repo.tar.gz"),
            "-C",
            extract_dir.to_str().unwrap_or("extracted"),
        ])
        .status()
        .map_err(|e| DocsFetchError::Failed(format!("tar failed: {}", e)))?;

    if !tar_status.success() {
        return Err(DocsFetchError::Failed(
            "failed to extract tarball".to_owned(),
        ));
    }

    // Find the extracted directory (tarball creates a prefix dir like `horsies-main/`).
    let prefix = fs::read_dir(&extract_dir)?
        .filter_map(|e| e.ok())
        .find(|e| e.path().is_dir())
        .ok_or_else(|| DocsFetchError::Failed("tarball is empty".to_owned()))?;

    let docs_src = prefix.path().join(DOCS_REPO_PATH);
    let llms_src = prefix.path().join(LLMS_REPO_PATH);
    assemble_output(&docs_src, &llms_src, output_dir)
}

fn assemble_output(
    docs_src: &Path,
    llms_src: &Path,
    output_dir: &Path,
) -> Result<usize, DocsFetchError> {
    if !docs_src.is_dir() {
        return Err(DocsFetchError::Failed(format!(
            "docs source directory not found: {}",
            docs_src.display(),
        )));
    }

    // Atomic-ish swap: copy into a temp sibling first.
    let tmp_output = output_dir.with_extension("tmp");
    if tmp_output.exists() {
        fs::remove_dir_all(&tmp_output)?;
    }

    copy_dir_recursive(docs_src, &tmp_output)?;

    // Copy llms.txt if it exists (not fatal if missing).
    if llms_src.is_file() {
        fs::copy(llms_src, tmp_output.join("llms.txt"))?;
    }

    // Swap: remove old, rename new.
    if output_dir.exists() {
        fs::remove_dir_all(output_dir)?;
    }
    fs::rename(&tmp_output, output_dir)?;

    let file_count = count_files(output_dir);
    if file_count == 0 {
        return Err(DocsFetchError::Failed(
            "no files were copied to output directory".to_owned(),
        ));
    }

    Ok(file_count)
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<(), io::Error> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if src_path.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}

fn count_files(dir: &Path) -> usize {
    walkdir(dir).filter(|p| p.is_file()).count()
}

fn walkdir(dir: &Path) -> Box<dyn Iterator<Item = PathBuf>> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Box::new(std::iter::empty());
    };
    Box::new(entries.filter_map(|e| e.ok()).flat_map(|e| {
        let path = e.path();
        if path.is_dir() {
            let mut items = vec![];
            for p in walkdir(&path) {
                items.push(p);
            }
            items.into_iter().collect::<Vec<_>>().into_iter()
        } else {
            vec![path].into_iter()
        }
    }))
}

fn print_status(message: &str) {
    eprintln!("{}", message);
}
