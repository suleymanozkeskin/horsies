//! `package_version.ts`: the version of the npm package, from the `version`
//! member of its `package.json`.
//!
//! The telemetry of the package gives this version as the version of its
//! instrumentation scope. The version has one source, `package.json`:
//! `check` fails when the version changes and the module is not written
//! again. A read of `package.json` at run time is not possible in a bundled
//! app, so the package reads the generated module.
//!
//! The version has the form that the package build accepts:
//! `<major>.<minor>.<patch>`, each part ASCII digits, then optionally `-`
//! and one or more of `0-9`, `A-Z`, `a-z`, `.` and `-`.

use std::io::Read;
use std::path::{Path, PathBuf};

use horsies_core::JsonText;

use crate::output::{catalog_file, GeneratedFile};
use crate::ts_text::{module_text, string_literal};

/// The stem of the module: `src/_catalog/package_version.ts`.
pub const PACKAGE_VERSION_MODULE: &str = "package_version";

/// The manifest file of the npm package, in the package folder.
pub const PACKAGE_MANIFEST_FILE: &str = "package.json";

/// Most bytes read from [`PACKAGE_MANIFEST_FILE`]. A larger file is refused
/// rather than read without end.
pub const MAX_PACKAGE_MANIFEST_BYTES: u64 = 1_048_576;

/// The member of the manifest that holds the version.
const VERSION_MEMBER: &str = "version";

/// The constant that the module exports.
const VERSION_CONSTANT: &str = "PACKAGE_VERSION";

/// The count of the dot-separated number parts of a version.
const VERSION_NUMBER_PARTS: usize = 3;

/// The version of the npm package, in the form that the package build
/// accepts. Only [`parse_package_version`] constructs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageVersion(String);

impl PackageVersion {
    /// The version text.
    pub fn text(&self) -> &str {
        &self.0
    }
}

/// Why the text of a manifest gives no version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestRefusal {
    /// The text is not strict JSON.
    NotJson { cause: String },
    /// The JSON value is not an object.
    NotAnObject { cause: String },
    /// The object has no `version` member.
    NoVersion,
    /// The `version` member is not a text.
    VersionNotText { cause: String },
    /// The version text does not have the accepted form.
    VersionForm { version: String },
}

/// Why the version of a package folder did not read.
#[derive(Debug)]
pub enum PackageVersionError {
    /// The manifest file did not open or read.
    Read {
        path: PathBuf,
        cause: std::io::Error,
    },
    /// The manifest file has more than [`MAX_PACKAGE_MANIFEST_BYTES`] bytes.
    TooLarge { path: PathBuf, bound: u64 },
    /// The manifest text gives no version.
    Refused {
        path: PathBuf,
        refusal: ManifestRefusal,
    },
}

impl std::fmt::Display for PackageVersionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read { path, cause } => {
                write!(formatter, "the read of {} failed: {cause}", path.display())
            }
            Self::TooLarge { path, bound } => write!(
                formatter,
                "{} has more than {bound} bytes; it is not read",
                path.display()
            ),
            Self::Refused { path, refusal } => write!(
                formatter,
                "{} gives no package version: {refusal:?}",
                path.display()
            ),
        }
    }
}

/// The version of the npm package in `package_dir`, from its manifest.
///
/// Effects: reads at most [`MAX_PACKAGE_MANIFEST_BYTES`] + 1 bytes of the
/// manifest file. Changes nothing.
///
/// Errors: [`PackageVersionError`].
pub fn read_package_version(package_dir: &Path) -> Result<PackageVersion, PackageVersionError> {
    let path = package_dir.join(PACKAGE_MANIFEST_FILE);
    let file = std::fs::File::open(&path).map_err(|cause| PackageVersionError::Read {
        path: path.clone(),
        cause,
    })?;
    let mut text = String::new();
    let _byte_count = file
        .take(MAX_PACKAGE_MANIFEST_BYTES + 1)
        .read_to_string(&mut text)
        .map_err(|cause| PackageVersionError::Read {
            path: path.clone(),
            cause,
        })?;
    if text.len() as u64 > MAX_PACKAGE_MANIFEST_BYTES {
        return Err(PackageVersionError::TooLarge {
            path,
            bound: MAX_PACKAGE_MANIFEST_BYTES,
        });
    }
    parse_package_version(&text).map_err(|refusal| PackageVersionError::Refused { path, refusal })
}

/// The version of the manifest text `text`. Pure.
///
/// Errors: [`ManifestRefusal`].
pub fn parse_package_version(text: &str) -> Result<PackageVersion, ManifestRefusal> {
    let document = JsonText::parse(text).map_err(|cause| ManifestRefusal::NotJson {
        cause: cause.to_string(),
    })?;
    let members = document
        .decode::<std::collections::BTreeMap<String, JsonText>>()
        .map_err(|cause| ManifestRefusal::NotAnObject {
            cause: cause.to_string(),
        })?;
    let version = members
        .get(VERSION_MEMBER)
        .ok_or(ManifestRefusal::NoVersion)?
        .decode::<String>()
        .map_err(|cause| ManifestRefusal::VersionNotText {
            cause: cause.to_string(),
        })?;
    match has_version_form(&version) {
        true => Ok(PackageVersion(version)),
        false => Err(ManifestRefusal::VersionForm { version }),
    }
}

/// Whether `version` is `<major>.<minor>.<patch>`, each part one or more
/// ASCII digits, then optionally `-` and one or more of `0-9`, `A-Z`,
/// `a-z`, `.` and `-`. Pure.
fn has_version_form(version: &str) -> bool {
    let (numbers, suffix) = match version.split_once('-') {
        Some((numbers, suffix)) => (numbers, Some(suffix)),
        None => (version, None),
    };
    let parts: Vec<&str> = numbers.split('.').collect();
    let numbers_valid = parts.len() == VERSION_NUMBER_PARTS
        && parts
            .iter()
            .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()));
    let suffix_valid = match suffix {
        None => true,
        Some(suffix) => {
            !suffix.is_empty()
                && suffix
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'-')
        }
    };
    numbers_valid && suffix_valid
}

/// `package_version.ts` of `version`. Pure.
pub fn generated_file(version: &PackageVersion) -> GeneratedFile {
    catalog_file(
        PACKAGE_VERSION_MODULE,
        module_text(&[format!(
            "export const {VERSION_CONSTANT} = {};",
            string_literal(version.text())
        )]),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ts_text::GENERATED_HEADER;

    /// A fresh, empty folder under the system temp folder for one test.
    fn scratch_folder(test: &str) -> PathBuf {
        let folder = std::env::temp_dir().join(format!(
            "horsies-typescript-codegen-version-{test}-{}",
            std::process::id()
        ));
        let _absent_is_fine = std::fs::remove_dir_all(&folder);
        std::fs::create_dir_all(&folder).expect("the scratch folder is created");
        folder
    }

    fn version_of(text: &str) -> PackageVersion {
        parse_package_version(text).expect("the manifest has a version")
    }

    #[test]
    fn the_module_exports_the_version_text() {
        let file = generated_file(&version_of(r#"{"name": "horsies", "version": "0.8.1"}"#));

        assert_eq!(
            file.relative_path(),
            Path::new("src/_catalog/package_version.ts")
        );
        assert_eq!(
            file.text(),
            format!("{GENERATED_HEADER}\n\nexport const PACKAGE_VERSION = \"0.8.1\";\n")
        );
    }

    #[test]
    fn a_version_with_a_prerelease_suffix_is_accepted() {
        assert_eq!(
            version_of(r#"{"version": "1.0.0-rc.1"}"#).text(),
            "1.0.0-rc.1"
        );
    }

    #[test]
    fn a_text_that_is_not_json_is_refused() {
        assert!(matches!(
            parse_package_version("{\"version\": "),
            Err(ManifestRefusal::NotJson { .. })
        ));
    }

    #[test]
    fn a_value_that_is_not_an_object_is_refused() {
        assert!(matches!(
            parse_package_version("[\"0.8.1\"]"),
            Err(ManifestRefusal::NotAnObject { .. })
        ));
    }

    #[test]
    fn an_object_without_a_version_is_refused() {
        assert_eq!(
            parse_package_version(r#"{"name": "horsies"}"#),
            Err(ManifestRefusal::NoVersion)
        );
    }

    #[test]
    fn a_version_that_is_not_a_text_is_refused() {
        assert!(matches!(
            parse_package_version(r#"{"version": 8}"#),
            Err(ManifestRefusal::VersionNotText { .. })
        ));
    }

    #[test]
    fn a_version_of_another_form_is_refused() {
        for version in [
            "0.8",
            "0.8.1.2",
            "v0.8.1",
            "0.8.x",
            "0.8.1-",
            "0.8.1-a b",
            "",
        ] {
            assert_eq!(
                parse_package_version(&format!("{{\"version\": \"{version}\"}}")),
                Err(ManifestRefusal::VersionForm {
                    version: version.to_owned()
                }),
                "{version}"
            );
        }
    }

    #[test]
    fn the_version_of_a_package_folder_is_read_from_its_manifest() {
        let folder = scratch_folder("read");
        std::fs::write(
            folder.join(PACKAGE_MANIFEST_FILE),
            r#"{"name": "horsies", "version": "2.3.4"}"#,
        )
        .expect("the manifest is written");

        let version = read_package_version(&folder).expect("the version reads");

        assert_eq!(version.text(), "2.3.4");
    }

    #[test]
    fn a_folder_without_a_manifest_is_a_read_error() {
        let folder = scratch_folder("missing");

        assert!(matches!(
            read_package_version(&folder),
            Err(PackageVersionError::Read { .. })
        ));
    }

    #[test]
    fn a_manifest_past_the_bound_is_not_read() {
        let folder = scratch_folder("large");
        let padding =
            " ".repeat(usize::try_from(MAX_PACKAGE_MANIFEST_BYTES).expect("the bound fits"));
        std::fs::write(
            folder.join(PACKAGE_MANIFEST_FILE),
            format!("{{\"version\": \"1.0.0\"}}{padding}"),
        )
        .expect("the manifest is written");

        assert!(matches!(
            read_package_version(&folder),
            Err(PackageVersionError::TooLarge { .. })
        ));
    }

    #[test]
    fn a_refused_manifest_names_its_path() {
        let folder = scratch_folder("refused");
        std::fs::write(folder.join(PACKAGE_MANIFEST_FILE), r#"{"name": "horsies"}"#)
            .expect("the manifest is written");

        match read_package_version(&folder) {
            Err(PackageVersionError::Refused { path, refusal }) => {
                assert_eq!(path, folder.join(PACKAGE_MANIFEST_FILE));
                assert_eq!(refusal, ManifestRefusal::NoVersion);
            }
            other => panic!("{other:?}"),
        }
    }

    /// The version of the package of this repository has the accepted
    /// form, so `write` and `check` of the package folder read it.
    #[test]
    fn the_package_of_the_repository_has_a_version() {
        let package_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../horsies");

        let version = read_package_version(&package_dir).expect("the package version reads");

        assert!(!version.text().is_empty());
    }
}
