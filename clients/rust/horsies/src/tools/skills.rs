//! `horsies skills install --target <dir> [--force]`: copy the agent skill
//! folder of this release to `<dir>/horsies/` (Python `horsies skills
//! install`, `clients/python/horsies/core/skills_installer.py`).
//!
//! The files of `clients/rust/horsies/.agents/skills/horsies/` are in the binary,
//! put there when it was built. A copy that differs from them is replaced
//! only with `--force`. The new copy is written beside the destination
//! first and then moved into place, so a failure says what is at the
//! destination.

use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::Args;

/// The name of the skill folder inside the target.
const SKILL_FOLDER_NAME: &str = "horsies";
/// The deepest folder nesting that the read of an installed copy walks.
const MAX_SKILL_DIRECTORY_DEPTH: usize = 4;
/// The most files that the read of an installed copy reads.
const MAX_SKILL_FILE_COUNT: usize = 64;
/// The largest file of an installed copy that the read compares.
const MAX_SKILL_FILE_BYTES: u64 = 1024 * 1024;

/// The skill files of this release: relative path and content.
const PACKAGED_SKILL: [(&str, &[u8]); 5] = [
    (
        "SKILL.md",
        include_bytes!("../../.agents/skills/horsies/SKILL.md"),
    ),
    (
        "configs.md",
        include_bytes!("../../.agents/skills/horsies/configs.md"),
    ),
    (
        "monitoring.md",
        include_bytes!("../../.agents/skills/horsies/monitoring.md"),
    ),
    (
        "tasks.md",
        include_bytes!("../../.agents/skills/horsies/tasks.md"),
    ),
    (
        "workflows.md",
        include_bytes!("../../.agents/skills/horsies/workflows.md"),
    ),
];

/// The arguments of `horsies skills install`.
#[derive(Debug, Clone, Args)]
pub(super) struct InstallArgs {
    /// Skills directory of your agent tool, for example .claude/skills. The
    /// skill is written to <target>/horsies/.
    #[arg(long)]
    target: PathBuf,
    /// Replace an existing copy that differs from the skill of this release.
    #[arg(long)]
    force: bool,
}

/// How an install ended.
#[derive(Debug, Clone, PartialEq, Eq)]
enum InstallOutcome {
    /// The folder was written: it did not exist, or it differed and
    /// `--force` was given.
    Installed {
        destination: PathBuf,
        file_count: usize,
    },
    /// The folder holds exactly the skill files. Nothing was written.
    AlreadyCurrent { destination: PathBuf },
}

/// What is at the destination after a failed write.
#[derive(Debug, Clone, PartialEq, Eq)]
enum DestinationState {
    /// As before the call: the old copy, or no copy.
    Unchanged,
    /// The new copy is in place; the old copy is still at `backup`.
    NewCopyInPlaceBackupLeft { backup: PathBuf },
    /// No copy; the old copy is at `backup`.
    OldCopyAtBackup { backup: PathBuf },
}

/// Whether the staging folder of a failed write is gone.
#[derive(Debug, Clone, PartialEq, Eq)]
enum StagingState {
    Removed,
    LeftAt { path: PathBuf },
}

/// Why an install wrote nothing, or did not finish.
#[derive(Debug)]
enum InstallError {
    /// The target, or the skill folder inside it, is not a directory.
    /// Nothing was written.
    TargetNotDirectory { path: PathBuf },
    /// The destination differs from the skill files and `--force` was not
    /// given. Nothing was written.
    ChangedCopyExists {
        destination: PathBuf,
        differing_paths: Vec<String>,
    },
    /// A filesystem call failed.
    IoFailed {
        path: PathBuf,
        operation: &'static str,
        cause: std::io::Error,
        destination: DestinationState,
        staging: StagingState,
    },
}

/// The message that the command prints for `error` (Python
/// `describe_install_error`). Pure.
fn describe_install_error(error: &InstallError) -> String {
    match error {
        InstallError::TargetNotDirectory { path } => {
            format!("{} exists and is not a directory", path.display())
        }
        InstallError::ChangedCopyExists {
            destination,
            differing_paths,
        } => format!(
            "{} differs from the packaged skill ({}). Nothing was changed. Run again with \
             --force to replace it.",
            destination.display(),
            differing_paths.join(", ")
        ),
        InstallError::IoFailed {
            path,
            operation,
            cause,
            destination,
            staging,
        } => format!(
            "{operation} failed for {}: {cause}. {}{}",
            path.display(),
            describe_destination(destination),
            describe_staging(staging)
        ),
    }
}

fn describe_destination(state: &DestinationState) -> String {
    match state {
        DestinationState::Unchanged => "The destination is unchanged.".to_owned(),
        DestinationState::NewCopyInPlaceBackupLeft { backup } => format!(
            "The new copy is in place. The old copy is left at {}. Remove it by hand.",
            backup.display()
        ),
        DestinationState::OldCopyAtBackup { backup } => format!(
            "The destination has no copy. The old copy is at {}. Move it back by hand.",
            backup.display()
        ),
    }
}

fn describe_staging(state: &StagingState) -> String {
    match state {
        StagingState::Removed => String::new(),
        StagingState::LeftAt { path } => format!(
            " A staging folder is left at {}. Remove it by hand.",
            path.display()
        ),
    }
}

/// Run `skills install` and return its exit status: 0 when the folder is
/// written or already current, 1 on an [`InstallError`].
pub(super) fn run(args: &InstallArgs) -> ExitCode {
    match install_skill(&args.target, &packaged_files(), args.force) {
        Ok(InstallOutcome::Installed {
            destination,
            file_count,
        }) => {
            println!("installed {file_count} files to {}", destination.display());
            ExitCode::SUCCESS
        }
        Ok(InstallOutcome::AlreadyCurrent { destination }) => {
            println!("{} is already current", destination.display());
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("error: {}", describe_install_error(&error));
            ExitCode::FAILURE
        }
    }
}

/// The skill files of this release by relative path. Pure.
fn packaged_files() -> BTreeMap<String, Vec<u8>> {
    PACKAGED_SKILL
        .iter()
        .map(|(name, bytes)| ((*name).to_owned(), bytes.to_vec()))
        .collect()
}

/// Copy `packaged` to `<target>/horsies/` (Python `install_skill`).
///
/// - `Installed`: the destination was absent, or it differed and
///   `overwrite_changed` is set.
/// - `AlreadyCurrent`: the destination holds exactly `packaged`; nothing is
///   written.
///
/// Errors: [`InstallError`]. Every error but `IoFailed` leaves the
/// filesystem unchanged; `IoFailed` says what is at the destination.
fn install_skill(
    target: &Path,
    packaged: &BTreeMap<String, Vec<u8>>,
    overwrite_changed: bool,
) -> Result<InstallOutcome, InstallError> {
    let destination = target.join(SKILL_FOLDER_NAME);
    if target.exists() && !target.is_dir() {
        return Err(InstallError::TargetNotDirectory {
            path: target.to_path_buf(),
        });
    }
    if destination.exists() && !destination.is_dir() {
        return Err(InstallError::TargetNotDirectory { path: destination });
    }
    if destination.is_dir() {
        let differing = match read_installed_files(&destination) {
            Ok(installed) => differing_paths(packaged, &installed),
            Err(stopped_at) => vec![stopped_at],
        };
        match (differing.is_empty(), overwrite_changed) {
            (true, _) => return Ok(InstallOutcome::AlreadyCurrent { destination }),
            (false, false) => {
                return Err(InstallError::ChangedCopyExists {
                    destination,
                    differing_paths: differing,
                })
            }
            (false, true) => {}
        }
    }
    fs::create_dir_all(target)
        .map_err(|cause| unchanged_failure(target, "create the target", cause))?;
    write_swapped(&destination, packaged)?;
    Ok(InstallOutcome::Installed {
        destination,
        file_count: packaged.len(),
    })
}

/// The files of an installed copy by relative path, read with the bounds
/// of the skill folder.
///
/// Errors: the relative path where the read stopped (a bound passed, or a
/// file that cannot be read); the copy then counts as different there.
fn read_installed_files(destination: &Path) -> Result<BTreeMap<String, Vec<u8>>, String> {
    let mut files = BTreeMap::new();
    collect_files(destination, "", 0, &mut files)?;
    Ok(files)
}

fn collect_files(
    folder: &Path,
    prefix: &str,
    depth: usize,
    files: &mut BTreeMap<String, Vec<u8>>,
) -> Result<(), String> {
    if depth > MAX_SKILL_DIRECTORY_DEPTH {
        return Err(prefix.to_owned());
    }
    let entries = fs::read_dir(folder).map_err(|_unreadable| prefix.to_owned())?;
    let mut paths: Vec<PathBuf> = Vec::new();
    for entry in entries {
        paths.push(entry.map_err(|_unreadable| prefix.to_owned())?.path());
    }
    paths.sort();
    for path in paths {
        let name = path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        let relative = format!("{prefix}{name}");
        if path.is_dir() {
            collect_files(&path, &format!("{relative}/"), depth + 1, files)?;
            continue;
        }
        if files.len() >= MAX_SKILL_FILE_COUNT {
            return Err(relative);
        }
        let mut bytes = Vec::new();
        let read = fs::File::open(&path)
            .and_then(|file| file.take(MAX_SKILL_FILE_BYTES + 1).read_to_end(&mut bytes));
        match read.map(u64::try_from) {
            Ok(Ok(length)) if length <= MAX_SKILL_FILE_BYTES => {
                let _previous = files.insert(relative, bytes);
            }
            Ok(Ok(_) | Err(_)) | Err(_) => return Err(relative),
        }
    }
    Ok(())
}

/// The sorted relative paths where the two file sets differ: edited files,
/// missing files and files that the skill does not have. Pure.
fn differing_paths(
    packaged: &BTreeMap<String, Vec<u8>>,
    installed: &BTreeMap<String, Vec<u8>>,
) -> Vec<String> {
    let mut names: Vec<&String> = packaged.keys().chain(installed.keys()).collect();
    names.sort();
    names.dedup();
    names
        .into_iter()
        .filter(|name| packaged.get(*name) != installed.get(*name))
        .cloned()
        .collect()
}

fn unchanged_failure(path: &Path, operation: &'static str, cause: std::io::Error) -> InstallError {
    InstallError::IoFailed {
        path: path.to_path_buf(),
        operation,
        cause,
        destination: DestinationState::Unchanged,
        staging: StagingState::Removed,
    }
}

/// Write `packaged` into a staging folder beside `destination`, then move it
/// into place ([`swap_in`]). A failure removes the staging folder, or names
/// it when it cannot be removed.
///
/// Errors: [`InstallError::IoFailed`].
fn write_swapped(
    destination: &Path,
    packaged: &BTreeMap<String, Vec<u8>>,
) -> Result<(), InstallError> {
    let staging = sibling(destination, ".horsies-skill-");
    fs::create_dir(&staging)
        .map_err(|cause| unchanged_failure(&staging, "create the staging folder", cause))?;
    let written = write_staging(&staging, packaged).and_then(|()| swap_in(&staging, destination));
    match written {
        Ok(()) => Ok(()),
        Err(failure) => Err(with_staging_removed(failure, &staging)),
    }
}

fn write_staging(staging: &Path, packaged: &BTreeMap<String, Vec<u8>>) -> Result<(), InstallError> {
    for (relative, content) in packaged {
        let path = staging.join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|cause| unchanged_failure(parent, "write the new copy", cause))?;
        }
        fs::write(&path, content)
            .map_err(|cause| unchanged_failure(&path, "write the new copy", cause))?;
    }
    Ok(())
}

/// Move `staging` to `destination`. An old copy moves to a backup first;
/// the backup is removed after the new copy is in place. Each failure
/// states what is at the destination.
fn swap_in(staging: &Path, destination: &Path) -> Result<(), InstallError> {
    if !destination.exists() {
        return fs::rename(staging, destination)
            .map_err(|cause| unchanged_failure(destination, "install", cause));
    }
    let backup = sibling(destination, ".horsies-skill-old-");
    fs::rename(destination, &backup)
        .map_err(|cause| unchanged_failure(destination, "back up the old copy", cause))?;
    if let Err(cause) = fs::rename(staging, destination) {
        let destination_state = match fs::rename(&backup, destination) {
            Ok(()) => DestinationState::Unchanged,
            Err(_restore) => DestinationState::OldCopyAtBackup {
                backup: backup.clone(),
            },
        };
        return Err(InstallError::IoFailed {
            path: destination.to_path_buf(),
            operation: "install",
            cause,
            destination: destination_state,
            staging: StagingState::Removed,
        });
    }
    fs::remove_dir_all(&backup).map_err(|cause| InstallError::IoFailed {
        path: backup.clone(),
        operation: "remove the backup",
        cause,
        destination: DestinationState::NewCopyInPlaceBackupLeft {
            backup: backup.clone(),
        },
        staging: StagingState::Removed,
    })
}

/// `failure` after an attempt to remove `staging`: a staging folder that is
/// still there is named in it. A staging folder that was moved into place
/// is not there to remove.
fn with_staging_removed(failure: InstallError, staging: &Path) -> InstallError {
    let left = staging.exists() && fs::remove_dir_all(staging).is_err();
    match (failure, left) {
        (
            InstallError::IoFailed {
                path,
                operation,
                cause,
                destination,
                staging: _,
            },
            true,
        ) => InstallError::IoFailed {
            path,
            operation,
            cause,
            destination,
            staging: StagingState::LeftAt {
                path: staging.to_path_buf(),
            },
        },
        (failure, _) => failure,
    }
}

/// A path beside `path` named `<prefix><random hex>`. Pure but for the
/// random name.
fn sibling(path: &Path, prefix: &str) -> PathBuf {
    path.with_file_name(format!("{prefix}{}", uuid::Uuid::new_v4().simple()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn files(entries: &[(&str, &str)]) -> BTreeMap<String, Vec<u8>> {
        entries
            .iter()
            .map(|(name, text)| ((*name).to_owned(), text.as_bytes().to_vec()))
            .collect()
    }

    /// The embedded files are exactly the files of the skill folder, so a
    /// file added to the folder is not left out of the binary.
    #[test]
    fn the_embedded_skill_is_the_whole_skill_folder() {
        let folder = Path::new(env!("CARGO_MANIFEST_DIR")).join(".agents/skills/horsies");
        let on_disk = read_installed_files(&folder).expect("the skill folder reads");
        assert_eq!(on_disk, packaged_files());
        assert!(on_disk.contains_key("SKILL.md"));
    }

    #[test]
    fn a_new_target_gets_every_file() {
        let target = tempfile::tempdir().unwrap();
        let skill = files(&[("SKILL.md", "skill"), ("docs/tasks.md", "tasks")]);

        let outcome = install_skill(&target.path().join("skills"), &skill, false).unwrap();

        let destination = target.path().join("skills/horsies");
        assert_eq!(
            outcome,
            InstallOutcome::Installed {
                destination: destination.clone(),
                file_count: 2
            }
        );
        assert_eq!(read_installed_files(&destination).unwrap(), skill);
    }

    #[test]
    fn a_current_copy_is_left_as_it_is() {
        let target = tempfile::tempdir().unwrap();
        let skill = files(&[("SKILL.md", "skill")]);
        let _first = install_skill(target.path(), &skill, false).unwrap();

        let outcome = install_skill(target.path(), &skill, false).unwrap();

        assert_eq!(
            outcome,
            InstallOutcome::AlreadyCurrent {
                destination: target.path().join("horsies")
            }
        );
    }

    #[test]
    fn a_changed_copy_is_refused_without_force_and_replaced_with_it() {
        let target = tempfile::tempdir().unwrap();
        let skill = files(&[("SKILL.md", "skill"), ("tasks.md", "tasks")]);
        let _first = install_skill(target.path(), &skill, false).unwrap();
        let destination = target.path().join("horsies");
        fs::write(destination.join("tasks.md"), "edited").unwrap();
        fs::write(destination.join("notes.md"), "mine").unwrap();

        let refused = install_skill(target.path(), &skill, false).unwrap_err();
        match &refused {
            InstallError::ChangedCopyExists {
                differing_paths, ..
            } => assert_eq!(differing_paths, &["notes.md", "tasks.md"]),
            other => panic!("expected a changed copy, got {other:?}"),
        }
        assert!(describe_install_error(&refused).contains("--force"));
        assert_eq!(
            fs::read_to_string(destination.join("tasks.md")).unwrap(),
            "edited"
        );

        let replaced = install_skill(target.path(), &skill, true).unwrap();
        assert!(matches!(replaced, InstallOutcome::Installed { .. }));
        assert_eq!(read_installed_files(&destination).unwrap(), skill);
        let leftovers: Vec<String> = fs::read_dir(target.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(leftovers, ["horsies"]);
    }

    #[test]
    fn a_target_that_is_a_file_is_refused() {
        let target = tempfile::tempdir().unwrap();
        let file = target.path().join("skills");
        fs::write(&file, "not a folder").unwrap();

        let refused = install_skill(&file, &files(&[("SKILL.md", "s")]), true).unwrap_err();

        assert!(matches!(
            refused,
            InstallError::TargetNotDirectory { ref path } if path == &file
        ));
        let inner = target.path().join("other");
        fs::create_dir(&inner).unwrap();
        fs::write(inner.join("horsies"), "a file").unwrap();
        let refused = install_skill(&inner, &files(&[("SKILL.md", "s")]), true).unwrap_err();
        assert!(matches!(refused, InstallError::TargetNotDirectory { .. }));
    }

    #[test]
    fn each_failure_text_says_what_is_at_the_destination() {
        let left = InstallError::IoFailed {
            path: PathBuf::from("/t/horsies"),
            operation: "install",
            cause: std::io::Error::other("disk full"),
            destination: DestinationState::OldCopyAtBackup {
                backup: PathBuf::from("/t/.old"),
            },
            staging: StagingState::LeftAt {
                path: PathBuf::from("/t/.stage"),
            },
        };
        assert_eq!(
            describe_install_error(&left),
            "install failed for /t/horsies: disk full. The destination has no copy. The old \
             copy is at /t/.old. Move it back by hand. A staging folder is left at /t/.stage. \
             Remove it by hand."
        );
    }
}
