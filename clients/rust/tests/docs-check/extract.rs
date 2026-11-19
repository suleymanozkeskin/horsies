//! The extraction of the `rust` code blocks of the docs pages and the skill
//! files: the reading of each page with its block markers, and the module
//! files that `build.rs` writes. The rules of a block and of its markers are
//! in `src/lib.rs`. `tests/extract.rs` tests it.

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

/// The folders whose pages hold the checked blocks, relative to the
/// repository root.
const DOC_ROOTS: [&str; 2] = [
    "website/src/content/docs/rust",
    "crates/horsies/.agents/skills/horsies",
];

/// The repository root, relative to this crate.
const REPO_ROOT_FROM_CRATE: &str = "../../..";

/// The file extensions of a page.
const PAGE_EXTENSIONS: [&str; 2] = ["md", "mdx"];

/// The deepest folder below a docs root that the walk reads.
const MAX_FOLDER_DEPTH: usize = 8;

/// The most pages that the walk reads: a bound of the walk, far above the
/// count of the pages.
const MAX_PAGES: usize = 2_000;

/// The marker word of a check marker: `<!-- rust-check: ... -->` in `.md`,
/// `{/* rust-check: ... */}` in `.mdx`. The Python docs check reads
/// `check:` markers only, so the two checks never read each other's markers.
pub(crate) const MARKER_WORD: &str = "rust-check";

/// Why a block is not built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SkipKind {
    /// The block shortens on purpose: pseudo-code, a signature shown for
    /// reference, a "Wrong / Correct" pair.
    Elided,
    /// The block is correct but uses names that the page does not show.
    Partial,
    /// The block is wrong for the current API: a known docs defect.
    Defect,
}

/// How a block is built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BlockMode {
    /// Inside the module of the chained block before it on the page.
    Chained,
    /// In a module of its own that sees no other block.
    Alone,
    /// Not built.
    Skip(SkipKind),
}

/// One `rust` block of a page.
#[derive(Debug, Clone)]
pub(crate) struct Block {
    pub(crate) page: String,
    /// The line of the opening fence, from 1.
    pub(crate) line: usize,
    pub(crate) mode: BlockMode,
    pub(crate) code: String,
}

/// Why the extraction stops.
#[derive(Debug, Clone)]
pub(crate) enum ExtractError {
    /// Cargo did not set an environment variable of a build script.
    MissingEnv { name: &'static str },
    /// A file or folder operation failed.
    Io { path: String, cause: String },
    /// A docs root has a folder deeper than [`MAX_FOLDER_DEPTH`].
    TooDeep { path: String },
    /// The docs roots hold more than [`MAX_PAGES`] pages.
    TooManyPages,
    /// A marker line whose text is not a known marker.
    UnknownMarker {
        page: String,
        line: usize,
        text: String,
    },
    /// `rust-check: skip` with no kind.
    SkipWithoutKind { page: String, line: usize },
    /// `rust-check: skip KIND` with no reason.
    SkipWithoutReason { page: String, line: usize },
    /// A marker that is not directly above a `rust` fence.
    MarkerWithoutRustFence { page: String, line: usize },
    /// A fence that no closing fence ends.
    UnclosedFence { page: String, line: usize },
}

impl fmt::Display for ExtractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingEnv { name } => write!(formatter, "cargo did not set {name}"),
            Self::Io { path, cause } => write!(formatter, "{path}: {cause}"),
            Self::TooDeep { path } => write!(
                formatter,
                "{path}: the folder is more than {MAX_FOLDER_DEPTH} levels below its docs root"
            ),
            Self::TooManyPages => {
                write!(formatter, "the docs roots hold more than {MAX_PAGES} pages")
            }
            Self::UnknownMarker { page, line, text } => {
                write!(formatter, "{page}:{line}: unknown check marker: {text:?}")
            }
            Self::SkipWithoutKind { page, line } => write!(
                formatter,
                "{page}:{line}: \"{MARKER_WORD}: skip\" needs a kind: \
                 \"{MARKER_WORD}: skip elided|partial|defect -- reason\""
            ),
            Self::SkipWithoutReason { page, line } => write!(
                formatter,
                "{page}:{line}: \"{MARKER_WORD}: skip\" needs a reason: \
                 \"{MARKER_WORD}: skip KIND -- reason\""
            ),
            Self::MarkerWithoutRustFence { page, line } => write!(
                formatter,
                "{page}:{line}: the check marker is not directly above a rust fence"
            ),
            Self::UnclosedFence { page, line } => {
                write!(formatter, "{page}:{line}: the fence has no closing fence")
            }
        }
    }
}

/// What one line of a page is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LineKind<'a> {
    /// An opening or closing fence: its indent in bytes and its language.
    Fence { indent: usize, lang: &'a str },
    /// A check marker; the text between the marker word's colon and the
    /// comment's end, or `None` for a marker line that is not well formed.
    Marker { body: Option<&'a str> },
    /// Any other line.
    Text,
}

fn classify(line: &str) -> LineKind<'_> {
    let trimmed = line.trim_start();
    match trimmed.strip_prefix("```") {
        Some(rest) => {
            let lang_end = rest
                .find(|character: char| !(character.is_alphanumeric() || character == '_'))
                .unwrap_or(rest.len());
            LineKind::Fence {
                indent: line.len() - trimmed.len(),
                lang: &rest[..lang_end],
            }
        }
        None => classify_marker(trimmed.trim_end()),
    }
}

/// A marker line: `<!-- rust-check: BODY -->` or `{/* rust-check: BODY */}`.
fn classify_marker(trimmed: &str) -> LineKind<'_> {
    let comment = [("<!--", "-->"), ("{/*", "*/}")]
        .into_iter()
        .find_map(|(open, close)| trimmed.strip_prefix(open).map(|rest| (rest, close)));
    let Some((rest, close)) = comment else {
        return LineKind::Text;
    };
    let Some(after_word) = rest.trim_start().strip_prefix(MARKER_WORD) else {
        return LineKind::Text;
    };
    LineKind::Marker {
        body: after_word
            .strip_prefix(':')
            .and_then(|body| body.strip_suffix(close)),
    }
}

/// The mode of a marker body: `alone`, or `skip KIND -- reason`.
fn parse_marker(page: &str, line: usize, raw: &str, body: &str) -> Result<BlockMode, ExtractError> {
    let (head, reason) = match body.split_once("--") {
        Some((head, reason)) => (head, reason.trim()),
        None => (body, ""),
    };
    let words: Vec<&str> = head.split_whitespace().collect();
    match (words.as_slice(), reason.is_empty()) {
        (["alone"], _) => Ok(BlockMode::Alone),
        (["skip"], _) => Err(ExtractError::SkipWithoutKind {
            page: page.to_owned(),
            line,
        }),
        (["skip", _kind], true) => Err(ExtractError::SkipWithoutReason {
            page: page.to_owned(),
            line,
        }),
        (["skip", "elided"], false) => Ok(BlockMode::Skip(SkipKind::Elided)),
        (["skip", "partial"], false) => Ok(BlockMode::Skip(SkipKind::Partial)),
        (["skip", "defect"], false) => Ok(BlockMode::Skip(SkipKind::Defect)),
        _ => Err(ExtractError::UnknownMarker {
            page: page.to_owned(),
            line,
            text: raw.trim().to_owned(),
        }),
    }
}

/// The index of the closing fence of the fence above `from`, or `None` when
/// the page ends first.
fn closing_fence(lines: &[&str], from: usize) -> Option<usize> {
    (from..lines.len()).find(|&index| lines[index].trim_start().starts_with("```"))
}

/// `lines` without the first `indent` bytes of whitespace of each line. A
/// line whose whitespace does not end at a character boundary there is kept
/// as it is.
fn dedent(lines: &[&str], indent: usize) -> String {
    lines
        .iter()
        .map(|&line| {
            let leading = line.len() - line.trim_start().len();
            line.get(leading.min(indent)..).unwrap_or(line)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The `rust` blocks of one page, with their modes. Stops at the first
/// marker error or unclosed fence.
pub(crate) fn read_blocks(page: &str, text: &str) -> Result<Vec<Block>, ExtractError> {
    let lines: Vec<&str> = text.lines().collect();
    let mut blocks = Vec::new();
    let mut pending: Option<(usize, BlockMode)> = None;
    let mut index = 0;
    while index < lines.len() {
        let number = index + 1;
        let unplaced = |marker_line: usize| ExtractError::MarkerWithoutRustFence {
            page: page.to_owned(),
            line: marker_line,
        };
        match classify(lines[index]) {
            LineKind::Marker { body } => {
                if let Some((marker_line, _mode)) = pending {
                    return Err(unplaced(marker_line));
                }
                let mode = match body {
                    Some(body) => parse_marker(page, number, lines[index], body)?,
                    None => {
                        return Err(ExtractError::UnknownMarker {
                            page: page.to_owned(),
                            line: number,
                            text: lines[index].trim().to_owned(),
                        })
                    }
                };
                pending = Some((number, mode));
                index += 1;
            }
            LineKind::Fence { indent, lang } => {
                let end = closing_fence(&lines, index + 1).ok_or(ExtractError::UnclosedFence {
                    page: page.to_owned(),
                    line: number,
                })?;
                match (lang == "rust", pending.take()) {
                    (true, marker) => blocks.push(Block {
                        page: page.to_owned(),
                        line: number,
                        mode: marker.map_or(BlockMode::Chained, |(_line, mode)| mode),
                        code: dedent(&lines[index + 1..end], indent),
                    }),
                    (false, Some((marker_line, _mode))) => return Err(unplaced(marker_line)),
                    (false, None) => {}
                }
                index = end + 1;
            }
            LineKind::Text => {
                if let (Some((marker_line, _mode)), false) =
                    (pending, lines[index].trim().is_empty())
                {
                    return Err(unplaced(marker_line));
                }
                index += 1;
            }
        }
    }
    match pending {
        Some((marker_line, _mode)) => Err(ExtractError::MarkerWithoutRustFence {
            page: page.to_owned(),
            line: marker_line,
        }),
        None => Ok(blocks),
    }
}

/// The pages below `folder` (named `relative` from the repository root), in
/// path order, appended to `pages`. Prints a rerun line for each folder and
/// page.
fn collect_pages(
    folder: &Path,
    relative: &str,
    depth: usize,
    pages: &mut Vec<(PathBuf, String)>,
) -> Result<(), ExtractError> {
    if depth > MAX_FOLDER_DEPTH {
        return Err(ExtractError::TooDeep {
            path: relative.to_owned(),
        });
    }
    println!("cargo:rerun-if-changed={}", folder.display());
    let io = |cause: std::io::Error| ExtractError::Io {
        path: relative.to_owned(),
        cause: cause.to_string(),
    };
    let mut entries = fs::read_dir(folder)
        .map_err(io)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(io)?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        let child = format!("{relative}/{name}");
        let is_page = path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| PAGE_EXTENSIONS.contains(&extension));
        match (path.is_dir(), is_page) {
            (true, _) => collect_pages(&path, &child, depth + 1, pages)?,
            (false, true) if pages.len() >= MAX_PAGES => return Err(ExtractError::TooManyPages),
            (false, true) => {
                println!("cargo:rerun-if-changed={}", path.display());
                pages.push((path, child));
            }
            (false, false) => {}
        }
    }
    Ok(())
}

/// The module name of the block at `line` of `page`: the page path, its
/// extension dropped, each run of other characters than letters and digits
/// as one `_`, and the line.
pub(crate) fn module_name(page: &str, line: usize) -> String {
    let stem = page
        .rsplit_once('.')
        .map_or(page, |(stem, _extension)| stem)
        .to_lowercase();
    let mut name = String::with_capacity(stem.len());
    for character in stem.chars() {
        match (character.is_ascii_alphanumeric(), name.ends_with('_')) {
            (true, _) => name.push(character),
            (false, false) => name.push('_'),
            (false, true) => {}
        }
    }
    format!("{}__line{line}", name.trim_matches('_'))
}

/// One module file: a block, and the next block of its chain.
#[derive(Debug, Clone)]
pub(crate) struct ModuleFile {
    pub(crate) name: String,
    pub(crate) page: String,
    pub(crate) line: usize,
    pub(crate) code: String,
    /// Whether the module is inside the module of the block before it.
    pub(crate) nested: bool,
    /// The module of the next block of the chain.
    pub(crate) next: Option<String>,
}

/// The module files of the blocks of one page, in page order. A skipped
/// block has none.
pub(crate) fn page_modules(blocks: &[Block]) -> Vec<ModuleFile> {
    let mut modules: Vec<ModuleFile> = Vec::new();
    let mut chain_tail: Option<usize> = None;
    for block in blocks {
        let module = |nested: bool| ModuleFile {
            name: module_name(&block.page, block.line),
            page: block.page.clone(),
            line: block.line,
            code: block.code.clone(),
            nested,
            next: None,
        };
        match block.mode {
            BlockMode::Skip(_kind) => {}
            BlockMode::Alone => modules.push(module(false)),
            BlockMode::Chained => {
                let added = module(chain_tail.is_some());
                if let Some(tail) = chain_tail {
                    modules[tail].next = Some(added.name.clone());
                }
                chain_tail = Some(modules.len());
                modules.push(added);
            }
        }
    }
    modules
}

/// The `mod` item of the module `name`, whose file is in `folder`.
pub(crate) fn mod_item(folder: &Path, name: &str) -> String {
    let path = folder.join(format!("{name}.rs"));
    format!("#[path = {:?}]\nmod {name};\n", path.display().to_string())
}

/// The text of the file of `module`. Line 1 names the source; the block
/// starts at line 2, so line L of the file is line `L + module.line - 1` of
/// the page.
pub(crate) fn module_text(module: &ModuleFile, folder: &Path) -> String {
    let mut text = format!(
        "// The rust block at line {line} of {page}: line L of this file is line L + {offset} \
         of the page.\n{code}\n",
        line = module.line,
        page = module.page,
        offset = module.line - 1,
        code = module.code,
    );
    if module.nested {
        text.push_str("use super::*;\n");
    }
    if let Some(next) = &module.next {
        text.push_str(&mod_item(folder, next));
    }
    text
}

/// Write the module files of `modules` and `blocks.rs` into `out_dir`.
fn write_modules(out_dir: &Path, modules: &[ModuleFile]) -> Result<(), ExtractError> {
    let folder = out_dir.join("blocks");
    let io = |path: &Path| {
        let path = path.display().to_string();
        move |cause: std::io::Error| ExtractError::Io {
            path,
            cause: cause.to_string(),
        }
    };
    if folder.exists() {
        fs::remove_dir_all(&folder).map_err(io(&folder))?;
    }
    fs::create_dir_all(&folder).map_err(io(&folder))?;
    let mut roots = String::new();
    for module in modules {
        let file = folder.join(format!("{}.rs", module.name));
        fs::write(&file, module_text(module, &folder)).map_err(io(&file))?;
        if !module.nested {
            roots.push_str(&mod_item(&folder, &module.name));
        }
    }
    let blocks = out_dir.join("blocks.rs");
    fs::write(&blocks, roots).map_err(io(&blocks))
}

fn env_path(name: &'static str) -> Result<PathBuf, ExtractError> {
    std::env::var_os(name)
        .map(PathBuf::from)
        .ok_or(ExtractError::MissingEnv { name })
}

/// Read every page, then write the modules. Every page is read before the
/// errors are returned, so one run lists the errors of every page.
pub(crate) fn run() -> Result<(), Vec<ExtractError>> {
    println!("cargo:rerun-if-changed=build.rs");
    let manifest_dir = env_path("CARGO_MANIFEST_DIR").map_err(|error| vec![error])?;
    let out_dir = env_path("OUT_DIR").map_err(|error| vec![error])?;
    let repo_root = manifest_dir.join(REPO_ROOT_FROM_CRATE);
    let mut pages = Vec::new();
    for root in DOC_ROOTS {
        collect_pages(&repo_root.join(root), root, 0, &mut pages).map_err(|error| vec![error])?;
    }
    let mut errors = Vec::new();
    let mut modules = Vec::new();
    for (path, page) in &pages {
        let read = fs::read_to_string(path)
            .map_err(|cause| ExtractError::Io {
                path: page.clone(),
                cause: cause.to_string(),
            })
            .and_then(|text| read_blocks(page, &text));
        match read {
            Ok(blocks) => modules.extend(page_modules(&blocks)),
            Err(error) => errors.push(error),
        }
    }
    match errors.is_empty() {
        true => write_modules(&out_dir, &modules).map_err(|error| vec![error]),
        false => Err(errors),
    }
}
