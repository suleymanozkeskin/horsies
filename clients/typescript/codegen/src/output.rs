//! The set of generated files, and how they are written and checked.
//!
//! Every file is a module of [`CATALOG_FOLDER`]. `write` writes the set and
//! removes the modules of the folder that carry
//! [`GENERATED_HEADER`] but are not in the set. `check` compares the set
//! with the folder and changes nothing.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use crate::package_version::{self, PackageVersion};
use crate::ts_text::GENERATED_HEADER;
use crate::{
    catalogs, constants, error_catalogs, error_detail, events, manifest, monitoring_models,
    producer_metrics, retention_class, task_code, wire_json,
};

/// The folder of the generated modules, inside the package folder.
pub const CATALOG_FOLDER: &str = "src/_catalog";

/// The extension of a generated module.
pub const MODULE_EXTENSION: &str = "ts";

/// Most entries read from [`CATALOG_FOLDER`]. A folder with more is refused
/// rather than scanned without end.
pub const MAX_CATALOG_FOLDER_ENTRIES: usize = 1_024;

/// One generated module, relative to the package folder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GeneratedFile {
    relative_path: PathBuf,
    text: String,
}

impl GeneratedFile {
    /// The path of the module, relative to the package folder.
    pub fn relative_path(&self) -> &Path {
        &self.relative_path
    }

    /// The full text of the module.
    pub fn text(&self) -> &str {
        &self.text
    }
}

/// The module `<stem>.ts` of [`CATALOG_FOLDER`] with `text`. Pure.
pub fn catalog_file(stem: &str, text: String) -> GeneratedFile {
    GeneratedFile {
        relative_path: Path::new(CATALOG_FOLDER).join(format!("{stem}.{MODULE_EXTENSION}")),
        text,
    }
}

/// What a file operation was doing when it failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileOperation {
    Read,
    Write,
    CreateFolder,
    ListFolder,
    Remove,
}

/// Why writing or checking stopped.
#[derive(Debug)]
pub enum CodegenError {
    Io {
        path: PathBuf,
        operation: FileOperation,
        cause: std::io::Error,
    },
    TooManyEntries {
        folder: PathBuf,
        bound: usize,
    },
}

impl std::fmt::Display for CodegenError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io {
                path,
                operation,
                cause,
            } => write!(
                formatter,
                "{operation:?} of {} failed: {cause}",
                path.display()
            ),
            Self::TooManyEntries { folder, bound } => write!(
                formatter,
                "{} has more than {bound} entries; it is not scanned",
                folder.display()
            ),
        }
    }
}

/// What `check` found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CheckReport {
    Current,
    Stale {
        /// Files whose text differs from the generated text.
        differing: Vec<PathBuf>,
        /// Generated files that do not exist.
        missing: Vec<PathBuf>,
        /// Modules of the folder with the generated header that are no
        /// longer generated.
        extra: Vec<PathBuf>,
    },
}

/// What `write` did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteReport {
    pub written: usize,
    pub removed: Vec<PathBuf>,
}

/// Every generated file, in a fixed order: the files of each area, areas
/// in this order. Pure.
pub fn render_all() -> Vec<GeneratedFile> {
    [
        catalogs::generated_files(),
        task_code::generated_files(),
        error_detail::generated_files(),
        retention_class::generated_files(),
        error_catalogs::generated_files(),
        wire_json::generated_files(),
        manifest::generated_files(),
        constants::generated_files(),
        events::generated_files(),
        producer_metrics::generated_files(),
        manifest::schedule_pattern_files(),
        monitoring_models::generated_files(),
    ]
    .into_iter()
    .flatten()
    .collect()
}

/// Every file of the package of `version`: the files of [`render_all`],
/// then `package_version.ts`. `write` and `check` take this set. Pure.
pub fn render_package(version: &PackageVersion) -> Vec<GeneratedFile> {
    let mut files = render_all();
    files.push(package_version::generated_file(version));
    files
}

/// Write every file under `package_dir`, and remove the modules of
/// [`CATALOG_FOLDER`] that carry the generated header but are not in
/// `files`. Other files are never touched. After a failure, the files
/// written before it stay, and no extra module is removed.
pub fn write_all(package_dir: &Path, files: &[GeneratedFile]) -> Result<WriteReport, CodegenError> {
    let catalog_folder = package_dir.join(CATALOG_FOLDER);
    std::fs::create_dir_all(&catalog_folder).map_err(|cause| CodegenError::Io {
        path: catalog_folder.clone(),
        operation: FileOperation::CreateFolder,
        cause,
    })?;
    for file in files {
        let path = package_dir.join(file.relative_path());
        std::fs::write(&path, file.text()).map_err(|cause| CodegenError::Io {
            path: path.clone(),
            operation: FileOperation::Write,
            cause,
        })?;
    }
    let extra = extra_generated_files(package_dir, files)?;
    for path in &extra {
        std::fs::remove_file(path).map_err(|cause| CodegenError::Io {
            path: path.clone(),
            operation: FileOperation::Remove,
            cause,
        })?;
    }
    Ok(WriteReport {
        written: files.len(),
        removed: extra,
    })
}

/// Compare every file under `package_dir` with its generated text. Reads
/// only.
pub fn check_all(package_dir: &Path, files: &[GeneratedFile]) -> Result<CheckReport, CodegenError> {
    let mut differing = Vec::new();
    let mut missing = Vec::new();
    for file in files {
        let path = package_dir.join(file.relative_path());
        match std::fs::read_to_string(&path) {
            Ok(text) if text == file.text() => {}
            Ok(_other_text) => differing.push(path),
            Err(cause) if cause.kind() == ErrorKind::NotFound => missing.push(path),
            Err(cause) => {
                return Err(CodegenError::Io {
                    path,
                    operation: FileOperation::Read,
                    cause,
                })
            }
        }
    }
    let extra = extra_generated_files(package_dir, files)?;
    match (differing.is_empty(), missing.is_empty(), extra.is_empty()) {
        (true, true, true) => Ok(CheckReport::Current),
        _ => Ok(CheckReport::Stale {
            differing,
            missing,
            extra,
        }),
    }
}

/// The modules of [`CATALOG_FOLDER`] that start with the generated header
/// and are not in `files`, in path order. A missing folder has none.
fn extra_generated_files(
    package_dir: &Path,
    files: &[GeneratedFile],
) -> Result<Vec<PathBuf>, CodegenError> {
    let catalog_folder = package_dir.join(CATALOG_FOLDER);
    let entries = match std::fs::read_dir(&catalog_folder) {
        Ok(entries) => entries,
        Err(cause) if cause.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(cause) => {
            return Err(CodegenError::Io {
                path: catalog_folder,
                operation: FileOperation::ListFolder,
                cause,
            })
        }
    };
    let generated: Vec<PathBuf> = files
        .iter()
        .map(|file| package_dir.join(file.relative_path()))
        .collect();
    let mut extra = Vec::new();
    for (position, entry) in entries.enumerate() {
        if position >= MAX_CATALOG_FOLDER_ENTRIES {
            return Err(CodegenError::TooManyEntries {
                folder: catalog_folder,
                bound: MAX_CATALOG_FOLDER_ENTRIES,
            });
        }
        let path = entry
            .map_err(|cause| CodegenError::Io {
                path: catalog_folder.clone(),
                operation: FileOperation::ListFolder,
                cause,
            })?
            .path();
        let is_module = path
            .extension()
            .is_some_and(|extension| extension == MODULE_EXTENSION);
        if !is_module || generated.contains(&path) {
            continue;
        }
        let text = std::fs::read_to_string(&path).map_err(|cause| CodegenError::Io {
            path: path.clone(),
            operation: FileOperation::Read,
            cause,
        })?;
        if text.lines().next() == Some(GENERATED_HEADER) {
            extra.push(path);
        }
    }
    extra.sort();
    Ok(extra)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    /// A fresh, empty folder under the system temp folder for one test.
    fn scratch_folder(test: &str) -> PathBuf {
        let folder = std::env::temp_dir().join(format!(
            "horsies-typescript-codegen-{test}-{}",
            std::process::id()
        ));
        let _absent_is_fine = std::fs::remove_dir_all(&folder);
        std::fs::create_dir_all(&folder).expect("the scratch folder is created");
        folder
    }

    /// Two modules with the generated header, as a renderer gives them.
    fn sample_files() -> Vec<GeneratedFile> {
        vec![
            catalog_file(
                "status",
                format!("{GENERATED_HEADER}\n\nexport const A = 1;\n"),
            ),
            catalog_file(
                "broker",
                format!("{GENERATED_HEADER}\n\nexport const B = 2;\n"),
            ),
        ]
    }

    #[test]
    fn a_catalog_file_is_a_module_of_the_catalog_folder() {
        let file = catalog_file("task_send_errors", "text\n".to_owned());
        assert_eq!(
            file.relative_path(),
            Path::new("src/_catalog/task_send_errors.ts")
        );
        assert_eq!(file.text(), "text\n");
    }

    #[test]
    fn every_rendered_file_is_rendered_once_with_the_header() {
        let files = render_all();
        let paths: BTreeSet<&Path> = files.iter().map(GeneratedFile::relative_path).collect();
        assert_eq!(paths.len(), files.len());
        for file in &files {
            assert!(
                file.text().starts_with(&format!("{GENERATED_HEADER}\n")),
                "{}",
                file.relative_path().display()
            );
            assert!(
                file.text().ends_with('\n') && !file.text().ends_with("\n\n"),
                "{} ends with one newline",
                file.relative_path().display()
            );
            assert!(file.relative_path().starts_with(CATALOG_FOLDER));
        }
    }

    #[test]
    fn the_output_set_is_every_module_of_the_catalog_folder() {
        let stems: BTreeSet<String> = render_all()
            .iter()
            .map(|file| {
                file.relative_path()
                    .file_stem()
                    .expect("a generated file has a name")
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        let expected: BTreeSet<String> = [
            "broker",
            "broker_bounds",
            "broker_errors",
            "codec_bounds",
            "error_codes",
            "error_detail",
            "event_bounds",
            "events",
            "exit_codes",
            "history",
            "manifest",
            "monitoring",
            "monitoring_bounds",
            "monitoring_errors",
            "monitoring_models",
            "producer_metrics",
            "retention_class",
            "schedule_defaults",
            "schedule_pattern",
            "scheduler_manifest",
            "send_bounds",
            "status",
            "task_code",
            "task_codes",
            "task_send",
            "task_send_errors",
            "wire_json",
            "worker_manifest",
            "workflow_bounds",
            "workflow_handle",
            "workflow_handle_errors",
            "workflow_spec",
            "workflow_start",
            "workflow_start_errors",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        assert_eq!(stems, expected);
    }

    #[test]
    fn the_package_set_is_the_rendered_set_and_the_package_version() {
        let version = package_version::parse_package_version(r#"{"version": "1.2.3"}"#)
            .expect("the manifest has a version");

        let files = render_package(&version);

        let mut expected = render_all();
        expected.push(package_version::generated_file(&version));
        assert_eq!(files, expected);
        assert_eq!(
            files.last().map(GeneratedFile::relative_path),
            Some(Path::new("src/_catalog/package_version.ts"))
        );
    }

    #[test]
    fn a_written_package_checks_as_current() {
        let folder = scratch_folder("current");
        let files = sample_files();
        let written = write_all(&folder, &files).expect("the files are written");
        assert_eq!(written.written, files.len());
        assert_eq!(written.removed, Vec::<PathBuf>::new());
        assert_eq!(
            check_all(&folder, &files).expect("the check reads"),
            CheckReport::Current
        );
    }

    #[test]
    fn a_changed_a_missing_and_an_extra_file_are_stale() {
        let folder = scratch_folder("stale");
        let files = sample_files();
        let _written = write_all(&folder, &files).expect("the files are written");
        let changed = folder.join(files[0].relative_path());
        std::fs::write(&changed, "edited").expect("the file is edited");
        let removed = folder.join(files[1].relative_path());
        std::fs::remove_file(&removed).expect("the file is removed");
        let extra = folder.join(CATALOG_FOLDER).join("retired.ts");
        std::fs::write(&extra, format!("{GENERATED_HEADER}\n")).expect("the extra is written");
        let handwritten = folder.join(CATALOG_FOLDER).join("handwritten.ts");
        std::fs::write(&handwritten, "export const X = 1;\n").expect("the other file is written");
        let other_extension = folder.join(CATALOG_FOLDER).join("notes.txt");
        std::fs::write(&other_extension, format!("{GENERATED_HEADER}\n"))
            .expect("the text file is written");

        let report = check_all(&folder, &files).expect("the check reads");

        assert_eq!(
            report,
            CheckReport::Stale {
                differing: vec![changed],
                missing: vec![removed],
                extra: vec![extra.clone()],
            }
        );
        let rewritten = write_all(&folder, &files).expect("the files are written again");
        assert_eq!(rewritten.removed, vec![extra]);
        assert!(handwritten.exists());
        assert!(other_extension.exists());
        assert_eq!(
            check_all(&folder, &files).expect("the check reads"),
            CheckReport::Current
        );
    }

    #[test]
    fn a_package_without_the_folder_has_every_file_missing() {
        let folder = scratch_folder("missing-folder");
        let files = sample_files();
        let expected: Vec<PathBuf> = files
            .iter()
            .map(|file| folder.join(file.relative_path()))
            .collect();

        let report = check_all(&folder, &files).expect("the check reads");

        assert_eq!(
            report,
            CheckReport::Stale {
                differing: Vec::new(),
                missing: expected,
                extra: Vec::new(),
            }
        );
    }

    #[test]
    fn a_folder_with_too_many_entries_is_not_scanned() {
        let folder = scratch_folder("too-many");
        let catalog_folder = folder.join(CATALOG_FOLDER);
        std::fs::create_dir_all(&catalog_folder).expect("the folder is created");
        for index in 0..=MAX_CATALOG_FOLDER_ENTRIES {
            std::fs::write(catalog_folder.join(format!("m{index}.txt")), "")
                .expect("the entry is written");
        }

        let outcome = check_all(&folder, &[]);

        match outcome {
            Err(CodegenError::TooManyEntries { folder, bound }) => {
                assert_eq!(folder, catalog_folder);
                assert_eq!(bound, MAX_CATALOG_FOLDER_ENTRIES);
            }
            other => panic!("expected TooManyEntries, got {other:?}"),
        }
    }
}
