//! The set of generated files, and how they are written and checked.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use horsies_core::catalog::CODE_CATALOGS;
use horsies_core::events::EVENT_CONTRACT;
use horsies_core::manifest_schema::{
    PRODUCER_MANIFEST, SCHEDULER_MANIFEST, WORKER_MANIFEST, WORKFLOW_SPEC_DATA,
};

use crate::catalogs::{group_by_module, render_module, render_package_init, CATALOG_FOLDER};
use crate::constants::{render_constant_module, CONSTANT_MODULES};
use crate::core_stub::{render_core_stub, CORE_STUB_FILE};
use crate::error_classes::{
    error_modules, exception_modules, render_error_detail_module, render_error_module,
    render_exception_module, render_new_type_module, ERROR_DETAIL_MODULE,
};
use crate::events::{render_events_module, EVENTS_MODULE_FILE};
use crate::manifest::{
    render_manifest_module, DumpEncoder, MANIFEST_MODULE_FILE, SCHEDULER_MANIFEST_MODULE_FILE,
    WORKER_MANIFEST_MODULE_FILE, WORKFLOW_SPEC_MODULE_FILE,
};
use crate::python_text::GENERATED_HEADER;
use crate::surface::CORE_MODULE;
use horsies_core::error_catalog::TEXT_NEW_TYPES;

/// Most entries read from the `_catalog` folder. A folder with more is
/// refused rather than scanned without end.
pub const MAX_CATALOG_FOLDER_ENTRIES: usize = 1_024;

/// One generated file, relative to the package folder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GeneratedFile {
    pub relative_path: PathBuf,
    pub text: String,
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
        /// Files of `_catalog` with the generated header that are no longer
        /// generated.
        extra: Vec<PathBuf>,
    },
}

/// What `write` did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteReport {
    pub written: usize,
    pub removed: Vec<PathBuf>,
}

/// Every generated file, in a fixed order.
pub fn render_all() -> Vec<GeneratedFile> {
    let modules = group_by_module(CODE_CATALOGS);
    let catalog_folder = Path::new(CATALOG_FOLDER);
    let mut files = vec![GeneratedFile {
        relative_path: catalog_folder.join("__init__.py"),
        text: render_package_init(&modules),
    }];
    for (module, catalogs) in &modules {
        files.push(GeneratedFile {
            relative_path: catalog_folder.join(format!("{module}.py")),
            text: render_module(module, catalogs),
        });
    }
    files.push(GeneratedFile {
        relative_path: catalog_folder.join(MANIFEST_MODULE_FILE),
        text: render_manifest_module(&PRODUCER_MANIFEST, DumpEncoder::PlainJson),
    });
    files.push(GeneratedFile {
        relative_path: catalog_folder.join(WORKER_MANIFEST_MODULE_FILE),
        text: render_manifest_module(&WORKER_MANIFEST, DumpEncoder::PlainJson),
    });
    files.push(GeneratedFile {
        relative_path: catalog_folder.join(SCHEDULER_MANIFEST_MODULE_FILE),
        text: render_manifest_module(&SCHEDULER_MANIFEST, DumpEncoder::PlainJson),
    });
    files.push(GeneratedFile {
        relative_path: catalog_folder.join(WORKFLOW_SPEC_MODULE_FILE),
        // The spec data holds user text (names, keys, node inputs): it is
        // written by the encoder of every stored form.
        text: render_manifest_module(&WORKFLOW_SPEC_DATA, DumpEncoder::StrictJsonIo),
    });
    for module in &CONSTANT_MODULES {
        files.push(GeneratedFile {
            relative_path: catalog_folder.join(module.file),
            text: render_constant_module(module),
        });
    }
    files.push(GeneratedFile {
        relative_path: catalog_folder.join(format!("{ERROR_DETAIL_MODULE}.py")),
        text: render_error_detail_module(),
    });
    for new_type in TEXT_NEW_TYPES {
        files.push(GeneratedFile {
            relative_path: catalog_folder.join(format!("{}.py", new_type.python_module)),
            text: render_new_type_module(new_type),
        });
    }
    for (module, contents) in &error_modules() {
        files.push(GeneratedFile {
            relative_path: catalog_folder.join(format!("{module}.py")),
            text: render_error_module(module, contents),
        });
    }
    for (module, classes) in &exception_modules() {
        files.push(GeneratedFile {
            relative_path: catalog_folder.join(format!("{module}.py")),
            text: render_exception_module(module, classes),
        });
    }
    files.push(GeneratedFile {
        relative_path: catalog_folder.join(EVENTS_MODULE_FILE),
        text: render_events_module(&EVENT_CONTRACT),
    });
    files.push(GeneratedFile {
        relative_path: PathBuf::from(CORE_STUB_FILE),
        text: render_core_stub(&CORE_MODULE),
    });
    files
}

/// Write every file under `package_dir`, and remove the files of `_catalog`
/// that carry the generated header but are no longer generated. Other files
/// are never touched. After a failure, the files written before it stay.
pub fn write_all(package_dir: &Path, files: &[GeneratedFile]) -> Result<WriteReport, CodegenError> {
    let catalog_folder = package_dir.join(CATALOG_FOLDER);
    std::fs::create_dir_all(&catalog_folder).map_err(|cause| CodegenError::Io {
        path: catalog_folder.clone(),
        operation: FileOperation::CreateFolder,
        cause,
    })?;
    for file in files {
        let path = package_dir.join(&file.relative_path);
        std::fs::write(&path, &file.text).map_err(|cause| CodegenError::Io {
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
        let path = package_dir.join(&file.relative_path);
        match std::fs::read_to_string(&path) {
            Ok(text) if text == file.text => {}
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

/// The `.py` files of `_catalog` that start with the generated header and
/// are not in `files`. A missing folder has none.
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
        .map(|file| package_dir.join(&file.relative_path))
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
        let is_python = path.extension().is_some_and(|extension| extension == "py");
        if !is_python || generated.contains(&path) {
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
    use super::*;

    /// A fresh, empty folder under the system temp folder for one test.
    fn scratch_folder(test: &str) -> PathBuf {
        let folder =
            std::env::temp_dir().join(format!("horsies-codegen-{test}-{}", std::process::id()));
        let _absent_is_fine = std::fs::remove_dir_all(&folder);
        std::fs::create_dir_all(&folder).expect("the scratch folder is created");
        folder
    }

    #[test]
    fn every_catalog_module_and_the_stub_are_rendered() {
        let files = render_all();
        let modules = group_by_module(CODE_CATALOGS);
        // __init__.py, one file per catalog module, manifest.py,
        // worker_manifest.py, scheduler_manifest.py, workflow_spec.py,
        // one file per constant module, error_detail.py, one file per new
        // type, one file per error module, one file per exception-class
        // module, events.py, _core.pyi
        assert_eq!(
            files.len(),
            modules.len()
                + 7
                + CONSTANT_MODULES.len()
                + 1
                + TEXT_NEW_TYPES.len()
                + error_modules().len()
                + exception_modules().len()
        );
        let paths: std::collections::BTreeSet<&PathBuf> =
            files.iter().map(|file| &file.relative_path).collect();
        assert_eq!(paths.len(), files.len());
        for file in &files {
            assert!(file.text.starts_with(GENERATED_HEADER));
        }
    }

    #[test]
    fn a_written_package_checks_as_current() {
        let folder = scratch_folder("current");
        let files = render_all();
        let written = write_all(&folder, &files).expect("the files are written");
        assert_eq!(written.written, files.len());
        assert_eq!(
            check_all(&folder, &files).expect("the check reads"),
            CheckReport::Current
        );
    }

    #[test]
    fn a_changed_a_missing_and_an_extra_file_are_stale() {
        let folder = scratch_folder("stale");
        let files = render_all();
        let _written = write_all(&folder, &files).expect("the files are written");
        let changed = folder.join(&files[0].relative_path);
        std::fs::write(&changed, "edited").expect("the file is edited");
        let removed = folder.join(CORE_STUB_FILE);
        std::fs::remove_file(&removed).expect("the stub is removed");
        let extra = folder.join(CATALOG_FOLDER).join("retired.py");
        std::fs::write(&extra, format!("{GENERATED_HEADER}\n")).expect("the extra is written");
        let handwritten = folder.join(CATALOG_FOLDER).join("handwritten.py");
        std::fs::write(&handwritten, "x = 1\n").expect("the other file is written");

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
        assert_eq!(
            check_all(&folder, &files).expect("the check reads"),
            CheckReport::Current
        );
    }
}
